package bus

import (
	"sync"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	rutils "go.viam.com/rdk/utils"

	"walkie/internal/audiofmt"
	"walkie/internal/queue"
)

// channel is one named channel: a set of listeners, and at most one talker.
type channel struct {
	name string
	cfg  ChannelConfig

	// info stamps primer and keepalive chunks for this channel's listeners.
	info *rutils.AudioInfo

	hangover  time.Duration
	maxQueued int

	// subs is copy-on-write: subsMu guards writers, and readers load the
	// pointer. The fan-out path therefore takes no lock at all, so subscribing
	// or unsubscribing can never stall a talker mid-transmission.
	subsMu sync.Mutex
	subs   atomic.Pointer[[]*sub]

	// floorMu guards holder, holderTx and heldUntil together. They only make
	// sense as a set.
	floorMu   sync.Mutex
	holder    string    // "" means free; survives a release to reserve the hangover
	holderTx  *Tx       // non-nil only while a transmission is actually running
	heldUntil time.Time // hangover deadline; meaningful only when holderTx is nil

	// lastChunk feeds the watchdog. Written by the talker on every chunk.
	lastChunk atomic.Int64

	transmissions  atomic.Uint64
	busyRejections atomic.Uint64
	revocations    atomic.Uint64
}

func (c *channel) subscribers() []*sub {
	if p := c.subs.Load(); p != nil {
		return *p
	}
	return nil
}

func (c *channel) addSub(s *sub) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	cur := c.subscribers()
	next := make([]*sub, len(cur), len(cur)+1)
	copy(next, cur)
	next = append(next, s)
	c.subs.Store(&next)
}

func (c *channel) removeSub(s *sub) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	cur := c.subscribers()
	next := make([]*sub, 0, len(cur))
	for _, other := range cur {
		// Compared by identity, never by member name: a retune briefly holds two
		// live subscriptions for one member, and the new one must survive the
		// old one's teardown.
		if other != s {
			next = append(next, other)
		}
	}
	c.subs.Store(&next)
}

// fanout delivers one chunk to every listener but the talker's own, on the
// talker's goroutine: the floor guarantees one writer and Offer never blocks.
// AudioData is shared; treat it as read-only.
func (c *channel) fanout(chunk *audioin.AudioChunk, from string) {
	for _, s := range c.subscribers() {
		// Self-echo suppression. Without it a talker's own voice comes straight
		// back out of their own speaker while their microphone is live, which is
		// an acoustic feedback loop by construction.
		if s.member == from {
			continue
		}
		if queue.Offer(s.q, chunk) {
			s.dropped.Add(1)
		}
	}
}

// acquire takes the floor for member, or reports why it could not.
func (c *channel) acquire(member string, info *rutils.AudioInfo, format audiofmt.Format, now time.Time) (*Tx, error) {
	c.floorMu.Lock()
	defer c.floorMu.Unlock()

	switch {
	case c.holderTx != nil && c.holder != member:
		// First talker wins. This is the whole point of a walkie-talkie.
		c.busyRejections.Add(1)
		return nil, BusyError(c.holder)

	case c.holderTx != nil:
		// The same member is already talking: a stream we thought was live but is
		// not. Never lock a member out of a channel they already hold, so
		// supersede without reserving a hangover.
		c.revokeLocked(now, false)

	case c.holder != "" && c.holder != member && now.Before(c.heldUntil):
		// Hangover. The floor is free but reserved: the previous talker's own
		// pump tears its stream down after 400ms of quiet, so without this a
		// bystander could take the channel during a breath.
		c.busyRejections.Add(1)
		return nil, BusyError(c.holder)
	}

	tx := &Tx{
		ch:      c,
		member:  member,
		info:    info,
		format:  format,
		start:   now,
		revoked: make(chan struct{}),
	}
	c.holder, c.holderTx, c.heldUntil = member, tx, time.Time{}
	c.lastChunk.Store(now.UnixNano())
	c.transmissions.Add(1)
	return tx, nil
}

// releaseLocked frees the floor. When reserve is true the departing holder keeps
// first refusal for the hangover; when false the channel is open to anyone at
// once, as a reclaimed or cleared floor is.
func (c *channel) releaseLocked(now time.Time, reserve bool) {
	c.holderTx = nil
	if reserve {
		c.heldUntil = now.Add(c.hangover)
		return
	}
	c.holder = ""
	c.heldUntil = time.Time{}
}

// revokeLocked cuts a live transmission off and frees the floor at once, rather
// than waiting for the talker's handler to unwind.
func (c *channel) revokeLocked(now time.Time, reserve bool) (holder string) {
	if c.holderTx == nil {
		return ""
	}
	holder = c.holder
	c.holderTx.markRevoked()
	c.revocations.Add(1)
	c.releaseLocked(now, reserve)
	return holder
}

// reclaim frees the floor if its holder stopped sending, and reports whose it
// was. Mandatory: the viam rpc server sets no keepalive.ServerParameters, so a
// dead member's stream stays live for minutes.
func (c *channel) reclaim(now time.Time, idle time.Duration) string {
	c.floorMu.Lock()
	defer c.floorMu.Unlock()
	if c.holderTx == nil {
		return ""
	}
	if last := c.lastChunk.Load(); last != 0 && now.Sub(time.Unix(0, last)) < idle {
		return ""
	}
	return c.revokeLocked(now, false)
}

// clear frees the floor unconditionally.
func (c *channel) clear(now time.Time) string {
	c.floorMu.Lock()
	defer c.floorMu.Unlock()
	holder := c.revokeLocked(now, false)
	c.holder = ""
	c.heldUntil = time.Time{}
	return holder
}

// currentHolder reports who is talking right now, or "" if nobody is.
func (c *channel) currentHolder() string {
	c.floorMu.Lock()
	defer c.floorMu.Unlock()
	if c.holderTx == nil {
		return ""
	}
	return c.holder
}

// Tx is a member's hold on one channel's floor, for one transmission. Send and
// its seq/elapsed fields belong to the goroutine running PlayStream; only Close
// and markRevoked are safe elsewhere.
type Tx struct {
	ch     *channel
	member string
	info   *rutils.AudioInfo
	format audiofmt.Format

	start   time.Time
	elapsed time.Duration
	seq     int32

	revoked    chan struct{}
	revokeOnce sync.Once
	closeOnce  sync.Once
}

// Member reports who holds this transmission.
func (t *Tx) Member() string { return t.member }

// Revoked is closed when this transmission loses the floor.
func (t *Tx) Revoked() <-chan struct{} { return t.revoked }

func (t *Tx) markRevoked() {
	t.revokeOnce.Do(func() { close(t.revoked) })
}

// Send fans one chunk out to the channel's listeners. It never blocks.
func (t *Tx) Send(data []byte) error {
	select {
	case <-t.revoked:
		return ErrRevoked
	default:
	}
	if len(data) == 0 {
		// Nothing to carry, and forwarding it would look like a keepalive to
		// every listener.
		return nil
	}

	now := time.Now()
	t.ch.lastChunk.Store(now.UnixNano())

	startAt := t.start.Add(t.elapsed)
	t.elapsed += t.format.DurationForBytes(len(data))
	t.seq++

	t.ch.fanout(&audioin.AudioChunk{
		AudioData:                 data,
		AudioInfo:                 t.info,
		Sequence:                  t.seq,
		StartTimestampNanoseconds: startAt.UnixNano(),
		EndTimestampNanoseconds:   t.start.Add(t.elapsed).UnixNano(),
	}, t.member)
	return nil
}

// Close releases the floor. Idempotent, and safe to call on a transmission that
// was already revoked.
func (t *Tx) Close() {
	t.closeOnce.Do(func() {
		c := t.ch
		c.floorMu.Lock()
		defer c.floorMu.Unlock()

		// The identity guard. Revocation frees the floor at once, so by the time this
		// deferred Close runs somebody else may hold it. Comparing pointers -- not
		// names -- stops us clobbering them.
		if c.holderTx != t {
			return
		}
		c.releaseLocked(time.Now(), true)
	})
}
