// Package bus routes audio between the members of a named channel. The RDK
// provides no audio fan-out anywhere, so the routing lives here and the two hub
// endpoints are thin shells over it.
package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/logging"
	rutils "go.viam.com/rdk/utils"

	"github.com/DTCurrie/viam-comms/audio/pcm"
)

// Defaults for Config. Each is explained where it is used.
const (
	DefaultMaxQueued     = 8
	DefaultFloorHangover = 800 * time.Millisecond
	DefaultFloorIdle     = 2 * time.Second
	DefaultKeepalive     = 10 * time.Second

	// MinFloorHangover is the talking pump's end-of-transmission idle. A hangover
	// shorter than this would expire before the talker's own stream had finished
	// tearing down, which defeats the point.
	MinFloorHangover = 400 * time.Millisecond
)

// Config configures a Bus. Zero values take the defaults above.
type Config struct {
	Channels []ChannelConfig

	// MaxQueued bounds how much audio may wait for one listener before the oldest
	// is discarded. Keep it small: the RDK owns two of the five buffers
	// between mic and speaker, so a large value buys latency.
	MaxQueued int

	// FloorHangover is how long a departing talker keeps a right of first
	// refusal on the channel.
	FloorHangover time.Duration

	// FloorIdle is how long a transmission may send nothing before the watchdog
	// takes its floor away.
	FloorIdle time.Duration

	// Keepalive is how often an idle listener is sent a zero-length chunk. Zero
	// disables it, which is only safe if you never need to tell a quiet channel
	// from a wedged hub.
	Keepalive time.Duration

	// DefaultFormat stamps primer and keepalive chunks on channels that do not
	// declare a format of their own.
	DefaultFormat pcm.Format

	Logger logging.Logger
}

// ChannelConfig declares one channel.
type ChannelConfig struct {
	Name string

	// SampleRate and NumChannels, when both set, are enforced: a talker whose
	// format does not match is rejected. Nothing resamples, so a mismatch would
	// otherwise garble every listener at once.
	SampleRate  int
	NumChannels int
}

func (cc ChannelConfig) format() (pcm.Format, bool) {
	f := pcm.Format{SampleRateHz: cc.SampleRate, NumChannels: cc.NumChannels}
	return f, cc.SampleRate > 0 && cc.NumChannels > 0
}

// SubReq describes a listener joining a channel.
type SubReq struct {
	Channel string
	Member  string
	Codec   string
	// Duration honours GetAudio's durationSeconds. Zero means "until the caller
	// goes away", which is what a radio wants and what the data collector does
	// not.
	Duration time.Duration
}

// TxReq describes a member starting to talk.
type TxReq struct {
	Channel string
	Member  string
	Format  pcm.Format
	Info    *rutils.AudioInfo
}

// Bus owns every channel on one hub.
type Bus struct {
	cfg    Config
	logger logging.Logger

	chans map[string]*channel
	names []string

	// mu guards closed together with subscription registration, so a Subscribe
	// racing a Close cannot slip a listener in behind the shutdown sweep and
	// leak its goroutine.
	mu     sync.Mutex
	closed bool

	nextID atomic.Uint64
	wg     sync.WaitGroup
}

// New builds a Bus. It starts nothing; call Run.
func New(cfg Config) (*Bus, error) {
	if len(cfg.Channels) == 0 {
		return nil, errors.New("a bus needs at least one channel")
	}
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = DefaultMaxQueued
	}
	if cfg.FloorHangover <= 0 {
		cfg.FloorHangover = DefaultFloorHangover
	}
	if cfg.FloorHangover < MinFloorHangover {
		return nil, fmt.Errorf("floor hangover of %s is shorter than a talker's own "+
			"end-of-transmission idle (%s), so it would expire before the previous "+
			"transmission had finished closing", cfg.FloorHangover, MinFloorHangover)
	}
	if cfg.FloorIdle <= 0 {
		cfg.FloorIdle = DefaultFloorIdle
	}
	if cfg.Keepalive < 0 {
		cfg.Keepalive = 0
	} else if cfg.Keepalive == 0 {
		cfg.Keepalive = DefaultKeepalive
	}
	if err := cfg.DefaultFormat.Valid(); err != nil {
		return nil, fmt.Errorf("default format: %w", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.NewLogger("bus")
	}

	b := &Bus{
		cfg:    cfg,
		logger: cfg.Logger,
		chans:  make(map[string]*channel, len(cfg.Channels)),
	}
	for _, cc := range cfg.Channels {
		if cc.Name == "" {
			return nil, errors.New("every channel needs a name")
		}
		if _, dup := b.chans[cc.Name]; dup {
			return nil, fmt.Errorf("channel %q is declared more than once", cc.Name)
		}
		format := cfg.DefaultFormat
		if declared, ok := cc.format(); ok {
			if err := declared.Valid(); err != nil {
				return nil, fmt.Errorf("channel %q: %w", cc.Name, err)
			}
			format = declared
		} else if cc.SampleRate != 0 || cc.NumChannels != 0 {
			return nil, fmt.Errorf("channel %q: set both sample_rate and num_channels, or neither", cc.Name)
		}

		b.chans[cc.Name] = &channel{
			name:      cc.Name,
			cfg:       cc,
			info:      format.AudioInfo(rutils.CodecPCM16),
			hangover:  cfg.FloorHangover,
			maxQueued: cfg.MaxQueued,
		}
		b.names = append(b.names, cc.Name)
	}
	sort.Strings(b.names)
	return b, nil
}

// Channels lists the declared channel names, in a stable order.
func (b *Bus) Channels() []string {
	out := make([]string, len(b.names))
	copy(out, b.names)
	return out
}

// Has reports whether a channel is declared.
func (b *Bus) Has(name string) bool {
	_, ok := b.chans[name]
	return ok
}

// DefaultFormat is the format used by channels that declare none of their own.
func (b *Bus) DefaultFormat() pcm.Format { return b.cfg.DefaultFormat }

// ChannelFormat reports the format a channel carries.
func (b *Bus) ChannelFormat(name string) (pcm.Format, bool) {
	c, ok := b.chans[name]
	if !ok {
		return pcm.Format{}, false
	}
	if declared, ok := c.cfg.format(); ok {
		return declared, true
	}
	return b.cfg.DefaultFormat, true
}

func (b *Bus) lookup(name string) (*channel, error) {
	c, ok := b.chans[name]
	if !ok {
		return nil, UnknownChannelError(name, b.names)
	}
	return c, nil
}

// Run drives the floor watchdog until ctx is done. Keepalives are per-listener
// and run on their own goroutines, so this is the only background work the bus
// itself needs.
func (b *Bus) Run(ctx context.Context) {
	// A quarter of the idle window: fast enough that a reclaim is prompt,
	// slow enough to be free.
	tick := b.cfg.FloorIdle / 4
	if tick <= 0 {
		tick = DefaultFloorIdle / 4
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, name := range b.names {
				if holder := b.chans[name].reclaim(now, b.cfg.FloorIdle); holder != "" {
					b.logger.Warnw(
						"reclaimed a channel from a talker that stopped sending; it most "+
							"likely lost power or its network, which the hub cannot detect "+
							"any other way",
						"channel", name, "member", holder, "idle", b.cfg.FloorIdle.String())
				}
			}
		}
	}
}

// Subscribe registers a listener and returns the channel its audio arrives on,
// plus a function ending the subscription. The channel closes exactly once; the
// caller must range it to completion.
func (b *Bus) Subscribe(ctx context.Context, req SubReq) (chan *audioin.AudioChunk, func(), error) {
	if req.Codec != "" && !pcm.SupportedCodec(req.Codec) {
		// Say so rather than quietly ignoring it: there is no transcoding here,
		// so a caller asking for opus would otherwise get pcm16 and no warning.
		return nil, nil, fmt.Errorf("only %q is supported, got %q", rutils.CodecPCM16, req.Codec)
	}
	c, err := b.lookup(req.Channel)
	if err != nil {
		return nil, nil, err
	}

	s := &sub{
		id:      b.nextID.Add(1),
		channel: req.Channel,
		member:  req.Member,
		q:       make(chan *audioin.AudioChunk, b.cfg.MaxQueued),
		out:     make(chan *audioin.AudioChunk, 1),
		done:    make(chan struct{}),
		info:    c.info,
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, nil, ErrClosed
	}
	b.warnOnDuplicateMember(c, req.Member)
	c.addSub(s)
	b.wg.Add(1)
	b.mu.Unlock()

	go func() {
		defer b.wg.Done()
		defer c.removeSub(s)
		s.run(ctx, b.cfg.Keepalive, req.Duration)
	}()

	b.logger.Debugw("listener joined", "channel", req.Channel, "member", req.Member, "id", s.id)
	return s.out, s.stop, nil
}

// warnOnDuplicateMember is called with b.mu held. Member identity is
// self-asserted, so a duplicate is a real misconfiguration: the two share a
// floor identity and each is silenced for the other.
func (b *Bus) warnOnDuplicateMember(c *channel, member string) {
	if member == "" {
		return
	}
	for _, other := range c.subscribers() {
		if other.member == member {
			b.logger.Warnw(
				"two listeners on this channel claim the same member name; they will "+
					"share a talking floor and will not hear each other. Give every "+
					"radio a distinct \"member\"",
				"channel", c.name, "member", member)
			return
		}
	}
}

// Publish takes the channel's floor so the caller can talk. First talker wins:
// a second member gets an error wrapping ErrBusy.
func (b *Bus) Publish(_ context.Context, req TxReq) (*Tx, error) {
	c, err := b.lookup(req.Channel)
	if err != nil {
		return nil, err
	}
	if declared, ok := c.cfg.format(); ok && declared != req.Format {
		return nil, fmt.Errorf("%w: channel %q carries %s, but this transmission is %s",
			ErrFormat, c.name, declared.String(), req.Format.String())
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	b.mu.Unlock()

	info := req.Info
	if info == nil {
		info = req.Format.AudioInfo(rutils.CodecPCM16)
	}
	return c.acquire(req.Member, info, req.Format, time.Now())
}

// ClearFloor frees a channel's floor by hand, and reports whose it was.
func (b *Bus) ClearFloor(name string) (string, error) {
	c, err := b.lookup(name)
	if err != nil {
		return "", err
	}
	return c.clear(time.Now()), nil
}

// Close shuts the bus down. It never blocks on an RPC.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	// Snapshot under the lock so nothing can join behind us.
	var subs []*sub
	for _, name := range b.names {
		subs = append(subs, b.chans[name].subscribers()...)
	}
	b.mu.Unlock()

	now := time.Now()
	for _, name := range b.names {
		if holder := b.chans[name].clear(now); holder != "" {
			b.logger.Debugw("revoked a transmission on shutdown", "channel", name, "member", holder)
		}
	}

	// Stopping a listener closes its out channel, which the audioin server reads
	// as a clean end of stream. Listeners see an orderly EOF and reconnect,
	// rather than an error.
	for _, s := range subs {
		s.stop()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// Bounded on purpose. A writer's only blocking operation selects on its
		// done channel, so this should be instant; if it is not, saying so beats
		// hanging a reconfigure.
		b.logger.Warn("timed out waiting for listeners to stop")
	}
	return nil
}

// Stats is a point-in-time snapshot of every channel.
type Stats struct {
	Channels []ChannelStats
}

// ChannelStats is one channel's snapshot.
type ChannelStats struct {
	Name           string
	Format         string
	Listeners      int
	Members        []string
	Holder         string
	Transmissions  uint64
	BusyRejections uint64
	Revocations    uint64
	ChunksSent     uint64
	ChunksDropped  uint64
	Keepalives     uint64
}

// Stats snapshots the bus.
func (b *Bus) Stats() Stats {
	out := Stats{Channels: make([]ChannelStats, 0, len(b.names))}
	for _, name := range b.names {
		c := b.chans[name]
		subs := c.subscribers()

		cs := ChannelStats{
			Name:           name,
			Format:         pcm.Format{SampleRateHz: int(c.info.SampleRateHz), NumChannels: int(c.info.NumChannels)}.String(),
			Listeners:      len(subs),
			Members:        make([]string, 0, len(subs)),
			Holder:         c.currentHolder(),
			Transmissions:  c.transmissions.Load(),
			BusyRejections: c.busyRejections.Load(),
			Revocations:    c.revocations.Load(),
		}
		for _, s := range subs {
			cs.Members = append(cs.Members, s.member)
			cs.ChunksSent += s.sent.Load()
			cs.ChunksDropped += s.dropped.Load()
			cs.Keepalives += s.keepalives.Load()
		}
		sort.Strings(cs.Members)
		out.Channels = append(out.Channels, cs)
	}
	return out
}
