package bus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	rutils "go.viam.com/rdk/utils"
)

// sub is one listener on one channel. Producers only touch q -- bounded,
// drop-oldest, NEVER closed, which removes send-on-closed-channel bugs. Only the
// writer goroutine sends on out and closes it.
type sub struct {
	id      uint64
	channel string
	member  string

	q    chan *audioin.AudioChunk
	out  chan *audioin.AudioChunk
	done chan struct{}

	stopOnce sync.Once

	// info stamps the primer and keepalive chunks. Never nil: the RDK's audioin
	// collector reads chunk.AudioInfo.SampleRateHz unchecked when its buffer is
	// empty, which a zero-length chunk guarantees.
	info *rutils.AudioInfo

	sent       atomic.Uint64
	dropped    atomic.Uint64
	keepalives atomic.Uint64
}

// stop ends the subscription. Safe to call any number of times, from anywhere.
func (s *sub) stop() {
	s.stopOnce.Do(func() { close(s.done) })
}

// heartbeat carries no audio. Zero-length is the mechanism, not an optimisation:
// the pump drops it before any counter, so it is invisible but for
// last_heartbeat_age_ms. Real silence would not be.
func (s *sub) heartbeat() *audioin.AudioChunk {
	return &audioin.AudioChunk{AudioInfo: s.info}
}

// run is the writer goroutine. It owns out.
func (s *sub) run(ctx context.Context, keepalive time.Duration, duration time.Duration) {
	defer close(s.out)

	// Prime first, before anything else. The RDK's audioin client blocks on one
	// Recv before GetAudio returns, so on a quiet channel a listener would block
	// there and report itself disconnected.
	if !s.emit(ctx, s.heartbeat()) {
		return
	}

	var ticks <-chan time.Time
	if keepalive > 0 {
		t := time.NewTicker(keepalive)
		defer t.Stop()
		ticks = t.C
	}

	// A non-zero duration honours GetAudio's durationSeconds, which the RDK data
	// collector relies on to finalise a WAV file.
	var deadline <-chan time.Time
	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		deadline = timer.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-deadline:
			return
		case chunk := <-s.q:
			if !s.emit(ctx, chunk) {
				return
			}
			s.sent.Add(1)
		case <-ticks:
			if !s.emit(ctx, s.heartbeat()) {
				return
			}
			s.keepalives.Add(1)
		}
	}
}

// emit hands one chunk to the RPC handler and reports whether the subscription
// is alive. The select on ctx is mandatory: the RDK's audioin server stops
// reading without draining when its context ends.
func (s *sub) emit(ctx context.Context, chunk *audioin.AudioChunk) bool {
	select {
	case s.out <- chunk:
		return true
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	}
}
