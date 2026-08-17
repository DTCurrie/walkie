// Package pump moves audio from an audio_in source to an audio_out sink, and
// gates when it is allowed through. Nothing in the Viam audio API joins the
// two: GetAudio pulls, PlayStream pushes.
package pump

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/logging"
	rutils "go.viam.com/rdk/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/audiofmt"
	"walkie/internal/queue"
)

// Source is the subset of audioin.AudioIn the pump needs, so a resource
// dependency satisfies it directly and the RDK fake satisfies it in tests.
type Source interface {
	GetAudio(ctx context.Context, codec string, durationSeconds float32,
		previousTimestampNs int64, extra map[string]interface{}) (chan *audioin.AudioChunk, error)
}

// Sink is the subset of audioout.AudioOut the pump needs.
type Sink interface {
	PlayStream(ctx context.Context, info *rutils.AudioInfo,
		chunks <-chan []byte, extra map[string]interface{}) error
}

// GateMode decides when captured audio is allowed through to the sink.
type GateMode int32

const (
	// GateManual passes audio only while SetTalking(true) is in effect. This is
	// push-to-talk, and it is the default because it makes acoustic feedback
	// topologically impossible rather than merely unlikely.
	GateManual GateMode = iota
	// GateVox opens the gate when the incoming level crosses a threshold and
	// closes it after a hangover period. Hands-free, at the cost of clipping
	// quiet speech onsets.
	GateVox
	// GateOpen passes everything. Only safe when the two machines cannot hear
	// each other, or when using headphones.
	GateOpen
)

// ParseGateMode maps a config string onto a GateMode.
func ParseGateMode(s string) (GateMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "manual", "ptt", "push_to_talk":
		return GateManual, nil
	case "vox", "auto":
		return GateVox, nil
	case "open", "always":
		return GateOpen, nil
	default:
		return GateManual, errors.New(`gate_mode must be "manual", "vox", or "open"`)
	}
}

func (g GateMode) String() string {
	switch g {
	case GateVox:
		return "vox"
	case GateOpen:
		return "open"
	default:
		return "manual"
	}
}

// endOfTransmissionIdle is how long the sender waits with an empty queue before
// closing the PlayStream. It must exceed the source's chunk cadence, or
// transmissions would be rebuilt mid-utterance.
const endOfTransmissionIdle = 400 * time.Millisecond

// silentWarnAfter is how long a stream may deliver only digital silence before
// we warn. A microphone denied under macOS TCC produces well-formed zeros and
// no error, and nothing else looks like that.
const silentWarnAfter = 5 * time.Second

// sinkUnimplWarnInterval rate-limits the PlayStream-unsupported report: the gate
// does not close on failure, so the sender retries about ten times a second.
const sinkUnimplWarnInterval = 30 * time.Second

// backoffWarnInterval rate-limits the OnStreamError report. Holding the talk
// button on a channel somebody else owns would otherwise fill the log at the
// backoff rate.
const backoffWarnInterval = 10 * time.Second

// SinkUnimplementedMsg explains a gRPC Unimplemented from the sink. PlayStream
// is the only streaming entry point on audio_out; Play takes a whole buffer.
// Exported so discovery and a link agree.
const SinkUnimplementedMsg = "sink audio_out does not implement PlayStream, so no audio can be carried; " +
	"its module is too old or only implements Play. For viam:system-audio, PlayStream arrived in " +
	"0.0.7-rc3 -- 0.0.6 and earlier serve only Play, and the registry's default pin is not always the " +
	"newest version, so set the sink machine's system-audio version explicitly"

// Config configures a Pump. The zero value of each field falls back to a
// documented default in New.
type Config struct {
	Codec        string
	Reconnect    time.Duration
	MaxQueued    int
	GateMode     GateMode
	VoxThreshold float64
	VoxHangover  time.Duration
	StartTalking bool
	Logger       logging.Logger

	// ExpectFormat, if set, is the format the operator configured both machines
	// for. A mismatch is counted and warned about but still forwarded with its
	// true format; dropping would instead make it silent.
	ExpectFormat *audiofmt.Format

	// Extra is passed to every GetAudio and PlayStream call, carrying
	// {"channel", "member"}. Both audio APIs deliver it per-caller, which is what
	// lets one pair of hub endpoints serve every channel.
	Extra map[string]interface{}

	// OnStreamError classifies an error from the sink: a positive backoff
	// suppresses transmissions for that long, and msg becomes the reported error.
	// Without it a busy channel spins at ~50 RPCs/s.
	OnStreamError func(err error) (backoff time.Duration, msg string)
}

// Stats is a point-in-time snapshot of the pump, surfaced through the link's
// Status and DoCommand.
type Stats struct {
	Connected     bool
	GateOpen      bool
	GateMode      string
	Transmissions uint64
	Streams       uint64
	Reconnects    uint64
	ChunksIn      uint64
	ChunksOut     uint64
	ChunksDropped uint64
	BytesOut      uint64
	// FormatMismatch counts chunks this module could not carry at all (no
	// AudioInfo, or a codec other than pcm16). Those are dropped.
	FormatMismatch uint64
	// FormatUnexpected counts chunks whose format differed from the configured
	// expectation. Those are still forwarded.
	FormatUnexpected uint64
	// Suppressed counts chunks discarded without an attempt because
	// OnStreamError asked for a backoff -- on a walkie-talkie, the audio spoken
	// into a channel somebody else is holding.
	Suppressed    uint64
	PeakDBFS      float64
	SilentSeconds float64
	LastChunkAge  time.Duration
	// LastHeartbeatAge is how long ago the source last sent a zero-length chunk.
	// Those are invisible to every other counter, so a source using them as a
	// keepalive stays distinguishable from one that died.
	LastHeartbeatAge time.Duration
	LastErr          string
}

// Pump reads from a Source and writes to a Sink until its context is done.
type Pump struct {
	src Source
	cfg Config

	// sink is guarded because the link may hand over a newly-resolved sink,
	// but is otherwise read-only during a run.
	sink Sink

	queue chan item

	talking   atomic.Bool
	talkUntil atomic.Int64 // unix nanos; 0 means no deadline
	gateMode  atomic.Int32
	gateOpen  atomic.Bool

	connected        atomic.Bool
	transmissions    atomic.Uint64
	streams          atomic.Uint64
	reconnects       atomic.Uint64
	chunksIn         atomic.Uint64
	chunksOut        atomic.Uint64
	chunksDropped    atomic.Uint64
	bytesOut         atomic.Uint64
	formatMismatch   atomic.Uint64
	formatUnexpected atomic.Uint64
	suppressedChunks atomic.Uint64

	peakDBFS     atomic.Uint64 // math.Float64bits
	lastChunkAt  atomic.Int64  // unix nanos
	heartbeatAt  atomic.Int64  // unix nanos of the last zero-length chunk
	silentSince  atomic.Int64  // unix nanos of the current silence onset; 0 means not silent
	silentWarnAt atomic.Int64  // unix nanos of the last silence warning; rate-limits it
	sinkUnimplAt atomic.Int64  // unix nanos of the last PlayStream-unsupported report
	backoffAt    atomic.Int64  // unix nanos of the last backoff report; rate-limits it

	// blockedUntil is the unix-nano deadline set by OnStreamError, before which
	// the sender does not attempt a transmission.
	blockedUntil atomic.Int64

	// extra is the map handed to GetAudio and PlayStream. Swapped wholesale by
	// SetExtra and never mutated in place, so readers need no lock.
	extra atomic.Pointer[map[string]interface{}]

	// cancelMu guards the two stream cancels, which let a retune end an
	// in-flight stream so the loops reopen it with the current extra.
	cancelMu   sync.Mutex
	srcCancel  context.CancelFunc
	sinkCancel context.CancelFunc

	// wake shortcuts the reconnect delay, so a retune does not cost a second of
	// deafness waiting on a timer that is about to be irrelevant.
	wake chan struct{}

	errMu   sync.Mutex
	lastErr string

	// voxUntil is touched only by the drain goroutine.
	voxUntil time.Time
}

// New builds a Pump. It does not start anything; call Run.
func New(src Source, sink Sink, cfg Config) *Pump {
	if cfg.Codec == "" {
		cfg.Codec = rutils.CodecPCM16
	}
	if cfg.Reconnect <= 0 {
		cfg.Reconnect = time.Second
	}
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = 10
	}
	if cfg.VoxThreshold == 0 {
		cfg.VoxThreshold = -40
	}
	if cfg.VoxHangover <= 0 {
		cfg.VoxHangover = 800 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.NewLogger("pump")
	}

	p := &Pump{
		src:   src,
		sink:  sink,
		cfg:   cfg,
		queue: make(chan item, cfg.MaxQueued),
		wake:  make(chan struct{}, 1),
	}
	p.gateMode.Store(int32(cfg.GateMode))
	p.talking.Store(cfg.StartTalking)
	p.setPeak(audiofmt.SilentDBFS)
	p.SetExtra(cfg.Extra)
	return p
}

// SetExtra swaps the map passed to subsequent GetAudio and PlayStream calls,
// copying it so the caller may reuse theirs. Nothing in flight is disturbed;
// follow with ResetSource or ResetSink to apply now.
func (p *Pump) SetExtra(extra map[string]interface{}) {
	var cp map[string]interface{}
	if extra != nil {
		cp = make(map[string]interface{}, len(extra))
		for k, v := range extra {
			cp[k] = v
		}
	}
	p.extra.Store(&cp)
}

// Extra returns the map currently handed to the source and sink.
func (p *Pump) Extra() map[string]interface{} {
	if m := p.extra.Load(); m != nil {
		return *m
	}
	return nil
}

// ResetSource ends the in-flight source stream so Run's reconnect loop reopens
// it with the current Extra. This is safe only because of runOnce's
// range-to-completion invariant; a break would wedge it.
func (p *Pump) ResetSource() {
	p.cancelMu.Lock()
	cancel := p.srcCancel
	p.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wakeUp()
}

// ResetSink ends the in-flight transmission, so a retune releases the floor on
// the old channel rather than holding it to the hangover. The next transmission
// opens with the current Extra.
func (p *Pump) ResetSink() {
	p.cancelMu.Lock()
	cancel := p.sinkCancel
	p.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	// A backoff earned on the old channel says nothing about the new one.
	p.blockedUntil.Store(0)
}

func (p *Pump) wakeUp() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// SetTalking opens or closes the manual push-to-talk gate. It has no effect in
// vox or open mode.
func (p *Pump) SetTalking(on bool) {
	p.talking.Store(on)
	p.talkUntil.Store(0)
}

// SetTalkingFor opens the manual gate and closes it again after d.
func (p *Pump) SetTalkingFor(d time.Duration) {
	p.talking.Store(true)
	p.talkUntil.Store(time.Now().Add(d).UnixNano())
}

// Talking reports whether the manual gate is held open.
func (p *Pump) Talking() bool { return p.talking.Load() }

// SetGateMode changes the gating strategy at runtime.
func (p *Pump) SetGateMode(m GateMode) { p.gateMode.Store(int32(m)) }

// GateMode reports the current gating strategy.
func (p *Pump) GateMode() GateMode { return GateMode(p.gateMode.Load()) }

// Run drives the pump until ctx is done, reconnecting whenever the source
// stream ends. Drive it with goutils.NewBackgroundStoppableWorkers.
func (p *Pump) Run(ctx context.Context) {
	// The sender owns every blocking call to the sink, so that the drain loop
	// can never be held up by a slow speaker. See runOnce for why that matters.
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		p.sender(ctx)
	}()
	defer func() {
		<-senderDone
	}()

	first := true
	for ctx.Err() == nil {
		if !first {
			p.reconnects.Add(1)
		}
		first = false

		p.runOnce(ctx)

		timer := time.NewTimer(p.cfg.Reconnect)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-p.wake:
			// A retune. Reopening against the old channel first would waste a
			// whole reconnect interval being deaf on the new one.
			timer.Stop()
		case <-timer.C:
		}
	}
}

// runOnce holds one GetAudio stream open for as long as it lasts.
func (p *Pump) runOnce(ctx context.Context) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.cancelMu.Lock()
	p.srcCancel = cancel
	p.cancelMu.Unlock()
	defer func() {
		p.cancelMu.Lock()
		p.srcCancel = nil
		p.cancelMu.Unlock()
	}()

	// durationSeconds 0 means an infinite stream, ended only by cancelling the
	// context. The RDK client blocks on one Recv, so this does not return until
	// the first chunk lands -- hence not the constructor.
	ch, err := p.src.GetAudio(streamCtx, p.cfg.Codec, 0, 0, p.Extra())
	if err != nil {
		p.setErr(err)
		p.cfg.Logger.Warnw("could not open source audio stream", "error", err)
		return
	}

	p.streams.Add(1)
	p.connected.Store(true)
	p.setErr(nil)
	defer p.connected.Store(false)
	p.cfg.Logger.Infow("source audio stream opened", "codec", p.cfg.Codec)

	// INVARIANT: range to completion, never break. The audioin client's feed
	// goroutine sends blockingly with no ctx select, so abandoning this channel
	// leaks it forever. Every early exit below is a continue.
	for chunk := range ch {
		p.consume(chunk)
	}

	p.cfg.Logger.Infow("source audio stream ended")
}

// consume handles exactly one chunk. It must never block.
func (p *Pump) consume(chunk *audioin.AudioChunk) {
	if chunk == nil {
		return
	}
	if len(chunk.AudioData) == 0 {
		// A chunk carrying no audio is invisible to every counter below, which is why
		// the downlink uses one to prime and keep a channel alive. Zeros instead
		// would look exactly like a TCC-denied microphone.
		p.heartbeatAt.Store(time.Now().UnixNano())
		return
	}
	p.chunksIn.Add(1)
	now := time.Now()
	p.lastChunkAt.Store(now.UnixNano())

	format, err := audiofmt.FromAudioInfo(chunk.AudioInfo)
	if err != nil {
		p.formatMismatch.Add(1)
		p.setErr(err)
		// Deliberately a return, not a break: see the invariant in runOnce.
		return
	}

	if p.cfg.ExpectFormat != nil && *p.cfg.ExpectFormat != format {
		if p.formatUnexpected.Add(1) == 1 {
			p.cfg.Logger.Warnw(
				"source format differs from the configured expectation; audio is still "+
					"forwarded with its true format, but the two machines are not "+
					"configured alike",
				"configured", p.cfg.ExpectFormat.String(), "actual", format.String())
		}
	}

	peak := audiofmt.PeakDBFS(chunk.AudioData)
	p.setPeak(peak)
	p.trackSilence(peak, now)

	if !p.evaluateGate(peak, now) {
		// Muted, but still drained and still counted. A test asserts that
		// ChunksIn keeps climbing while the gate is shut.
		return
	}

	if queue.Offer(p.queue, item{data: chunk.AudioData, format: format}) {
		p.chunksDropped.Add(1)
	}
}

// evaluateGate decides whether this chunk may pass, and records the decision.
func (p *Pump) evaluateGate(peak float64, now time.Time) bool {
	open := false
	switch p.GateMode() {
	case GateOpen:
		open = true
	case GateVox:
		if peak >= p.cfg.VoxThreshold {
			p.voxUntil = now.Add(p.cfg.VoxHangover)
		}
		open = now.Before(p.voxUntil)
	default: // GateManual
		if until := p.talkUntil.Load(); until != 0 && now.UnixNano() >= until {
			p.talking.Store(false)
			p.talkUntil.Store(0)
		}
		open = p.talking.Load()
	}
	p.gateOpen.Store(open)
	return open
}

// trackSilence tracks unbroken digital silence, warning once it is diagnostic.
// Onset and warning clock are separate: one timestamp would saw-tooth
// silent_seconds and never show a long-dead microphone.
func (p *Pump) trackSilence(peak float64, now time.Time) {
	if peak > audiofmt.SilentDBFS {
		p.silentSince.Store(0)
		p.silentWarnAt.Store(0)
		return
	}
	if p.silentSince.CompareAndSwap(0, now.UnixNano()) {
		return
	}
	if now.Sub(time.Unix(0, p.silentSince.Load())) < silentWarnAfter {
		return
	}
	if last := p.silentWarnAt.Load(); last != 0 && now.Sub(time.Unix(0, last)) < silentWarnAfter {
		return
	}
	p.silentWarnAt.Store(now.UnixNano())
	p.cfg.Logger.Warnw(
		"source has delivered nothing but digital silence; on macOS this is what a "+
			"microphone denied under TCC looks like -- run viam-server from Terminal.app "+
			"in a GUI login session, not over SSH, and check Privacy & Security > Microphone",
		"seconds", silentWarnAfter.Seconds(),
	)
}

// sender owns every blocking interaction with the sink. It opens a PlayStream
// when audio starts flowing and closes it once the flow stops.
func (p *Pump) sender(ctx context.Context) {
	var pending *item
	for ctx.Err() == nil {
		first := pending
		pending = nil

		if first == nil {
			it, ok := p.next(ctx, 0)
			if !ok {
				continue
			}
			first = &it
		}

		// Backed off: discard rather than open a stream we expect to lose. The
		// drain loop is untouched, so ChunksIn keeps climbing and the source
		// never wedges.
		if p.backedOff() {
			continue
		}
		pending = p.transmit(ctx, *first)
	}
}

// backedOff reports whether OnStreamError has asked us to hold off, counting
// the chunk being discarded as it does.
func (p *Pump) backedOff() bool {
	until := p.blockedUntil.Load()
	if until == 0 {
		return false
	}
	if time.Now().UnixNano() >= until {
		p.blockedUntil.CompareAndSwap(until, 0)
		return false
	}
	p.suppressedChunks.Add(1)
	return true
}

// next waits for the queue to yield an item. A timeout of 0 waits indefinitely
// (bounded by ctx). It reports ok=false if ctx ended or the timeout elapsed.
func (p *Pump) next(ctx context.Context, timeout time.Duration) (item, bool) {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return item{}, false
		case it := <-p.queue:
			return it, true
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return item{}, false
	case it := <-p.queue:
		return it, true
	case <-timer.C:
		return item{}, false
	}
}

// transmit runs one PlayStream while audio keeps the same format, returning the
// item whose format changed so the caller reopens. A stream per press starts
// with empty buffers, resetting clock drift.
func (p *Pump) transmit(ctx context.Context, first item) *item {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.cancelMu.Lock()
	p.sinkCancel = cancel
	p.cancelMu.Unlock()
	defer func() {
		p.cancelMu.Lock()
		p.sinkCancel = nil
		p.cancelMu.Unlock()
	}()

	chunks := make(chan []byte)
	done := make(chan error, 1)

	info := first.format.AudioInfo(p.cfg.Codec)
	extra := p.Extra()
	go func() {
		done <- p.sink.PlayStream(streamCtx, info, chunks, extra)
	}()

	p.transmissions.Add(1)
	p.cfg.Logger.Debugw("transmission started", "format", first.format.String())

	var (
		changed   *item
		streamErr error
		ended     bool
	)
	cur := first
	for {
		select {
		case chunks <- cur.data:
			p.chunksOut.Add(1)
			p.bytesOut.Add(uint64(len(cur.data)))
		case err := <-done:
			// The sink gave up. Nothing reads chunks after PlayStream returns
			// (components/audioout/client.go:125), so without this case the next
			// send would block until pump shutdown, wedging the sender for good.
			streamErr, ended = err, true
		case <-streamCtx.Done():
			// Abandon the send; the close below unwinds the RDK client.
		}
		if ended || streamCtx.Err() != nil {
			break
		}

		next, ok := p.next(streamCtx, endOfTransmissionIdle)
		if !ok {
			// Either the context ended or audio stopped flowing for long enough
			// that this utterance is over.
			break
		}
		if next.format != cur.format {
			changed = &next
			break
		}
		cur = next
	}

	if !ended {
		// Closing makes the RDK client call CloseAndRecv, which blocks until the
		// speaker has drained. That wait happens here on the sender goroutine,
		// never on the drain loop.
		close(chunks)
		streamErr = <-done
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) && !errors.Is(streamErr, io.EOF) {
		p.reportStreamErr(streamErr)
	}
	p.cfg.Logger.Debugw("transmission ended")

	return changed
}

// reportStreamErr records why a transmission failed. A gRPC Unimplemented is
// permanent: the far audio_out does not serve PlayStream, retrying cannot fix
// it, and the raw error is three wrapped hops deep.
func (p *Pump) reportStreamErr(err error) {
	// The caller's classifier gets first refusal. It is how a walkie-talkie
	// radio says "someone else holds the floor, stop shouting" without this
	// package needing to know that channels or floors exist.
	if p.cfg.OnStreamError != nil {
		if backoff, msg := p.cfg.OnStreamError(err); backoff > 0 {
			p.blockedUntil.Store(time.Now().Add(backoff).UnixNano())
			p.setErr(errors.New(msg))
			if p.shouldReport(&p.backoffAt, backoffWarnInterval) {
				p.cfg.Logger.Infow(msg, "backoff", backoff.String(), "error", err)
			} else {
				p.cfg.Logger.Debugw(msg, "backoff", backoff.String(), "error", err)
			}
			return
		}
	}

	if status.Code(err) != codes.Unimplemented {
		p.setErr(err)
		p.cfg.Logger.Warnw("playback stream ended with an error", "error", err)
		return
	}

	p.setErr(errors.New(SinkUnimplementedMsg))

	// The gate stays open, so without this the sender would retry -- and log --
	// once per chunk.
	if !p.shouldReport(&p.sinkUnimplAt, sinkUnimplWarnInterval) {
		p.cfg.Logger.Debugw("sink still does not implement PlayStream", "error", err)
		return
	}
	p.cfg.Logger.Errorw(SinkUnimplementedMsg, "error", err)
}

// shouldReport rate-limits a repeating report to once per interval, stamping at
// when it says yes. Every failure mode here repeats at the chunk rate, so
// sharing this keeps them from drifting apart.
func (p *Pump) shouldReport(at *atomic.Int64, every time.Duration) bool {
	now := time.Now()
	if last := at.Load(); last != 0 && now.Sub(time.Unix(0, last)) < every {
		return false
	}
	at.Store(now.UnixNano())
	return true
}

func (p *Pump) setErr(err error) {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	if err == nil {
		p.lastErr = ""
		return
	}
	p.lastErr = err.Error()
}

func (p *Pump) setPeak(v float64) {
	p.peakDBFS.Store(math.Float64bits(v))
}

// Stats returns a snapshot of the pump's counters.
func (p *Pump) Stats() Stats {
	p.errMu.Lock()
	lastErr := p.lastErr
	p.errMu.Unlock()

	var age time.Duration
	if at := p.lastChunkAt.Load(); at != 0 {
		age = time.Since(time.Unix(0, at))
	}
	var heartbeat time.Duration
	if at := p.heartbeatAt.Load(); at != 0 {
		heartbeat = time.Since(time.Unix(0, at))
	}
	var silent float64
	if since := p.silentSince.Load(); since != 0 {
		silent = time.Since(time.Unix(0, since)).Seconds()
	}

	return Stats{
		Connected:        p.connected.Load(),
		GateOpen:         p.gateOpen.Load(),
		GateMode:         p.GateMode().String(),
		Transmissions:    p.transmissions.Load(),
		Streams:          p.streams.Load(),
		Reconnects:       p.reconnects.Load(),
		ChunksIn:         p.chunksIn.Load(),
		ChunksOut:        p.chunksOut.Load(),
		ChunksDropped:    p.chunksDropped.Load(),
		BytesOut:         p.bytesOut.Load(),
		FormatMismatch:   p.formatMismatch.Load(),
		FormatUnexpected: p.formatUnexpected.Load(),
		Suppressed:       p.suppressedChunks.Load(),
		PeakDBFS:         math.Float64frombits(p.peakDBFS.Load()),
		SilentSeconds:    silent,
		LastChunkAge:     age,
		LastHeartbeatAge: heartbeat,
		LastErr:          lastErr,
	}
}
