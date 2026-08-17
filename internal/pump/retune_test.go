package pump

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.viam.com/rdk/components/audioin"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/test"

	"github.com/DTCurrie/viam-comms/audio/pcm"
)

// extraSink records the extra map each PlayStream was opened with, and drains
// honestly so the pump's own contract is not the thing under test here.
type extraSink struct {
	mu     sync.Mutex
	extras []map[string]interface{}
	calls  atomic.Uint64
}

func (s *extraSink) PlayStream(ctx context.Context, _ *rutils.AudioInfo,
	chunks <-chan []byte, extra map[string]interface{},
) error {
	s.calls.Add(1)
	s.mu.Lock()
	s.extras = append(s.extras, extra)
	s.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-chunks:
			if !ok {
				return nil
			}
		}
	}
}

func (s *extraSink) lastExtra() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.extras) == 0 {
		return nil
	}
	return s.extras[len(s.extras)-1]
}

func channelOf(extra map[string]interface{}) string {
	if extra == nil {
		return ""
	}
	if v, ok := extra["channel"].(string); ok {
		return v
	}
	return ""
}

// TestExtraReachesBothEndpoints is the mechanism the whole hub design rests on:
// channel and member identity ride in the extra map, which both audio APIs
// deliver per-caller.
func TestExtraReachesBothEndpoints(t *testing.T) {
	src := newFakeSource()
	sink := &extraSink{}

	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	cfg.Extra = map[string]interface{}{"channel": "ops", "member": "alpha"}
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "the source stream to open", func() bool { return src.calls.Load() > 0 })
	test.That(t, channelOf(src.lastExtra()), test.ShouldEqual, "ops")

	eventually(t, "a transmission to start", func() bool { return sink.calls.Load() > 0 })
	test.That(t, channelOf(sink.lastExtra()), test.ShouldEqual, "ops")
}

// TestSetExtraIsCopied: a caller must be able to keep and reuse their map
// without silently retuning a live pump.
func TestSetExtraIsCopied(t *testing.T) {
	p := New(newFakeSource(), &memSink{}, testConfig(t))

	mine := map[string]interface{}{"channel": "ops"}
	p.SetExtra(mine)
	mine["channel"] = "logistics"

	test.That(t, channelOf(p.Extra()), test.ShouldEqual, "ops")
}

// TestResetSourceReopensWithTheNewExtra is a retune. Cancelling the in-flight
// stream is the only safe way to do it -- the range-to-completion invariant
// means we can never break out of the chunk loop -- and the reconnect loop is
// what reopens it.
func TestResetSourceReopensWithTheNewExtra(t *testing.T) {
	src := newFakeSource()
	cfg := testConfig(t)
	cfg.Extra = map[string]interface{}{"channel": "ops"}
	p := New(src, &memSink{}, cfg)
	defer run(t, p)()

	eventually(t, "the first stream to open", func() bool { return src.calls.Load() > 0 })
	producedBefore := src.produced.Load()

	p.SetExtra(map[string]interface{}{"channel": "logistics"})
	p.ResetSource()

	eventually(t, "the stream to reopen on the new channel", func() bool {
		return channelOf(src.lastExtra()) == "logistics"
	})

	// The source must still be alive: a retune that wedged the microphone would
	// leave produced frozen here.
	eventually(t, "audio to keep flowing after the retune", func() bool {
		return src.produced.Load() > producedBefore+5
	})
}

// TestResetSourceWhileBlockedInGetAudio covers the case the primer normally
// hides: a retune issued while the pump is parked inside GetAudio's first Recv.
func TestResetSourceWhileBlockedInGetAudio(t *testing.T) {
	src := newFakeSource()
	src.blockUntilDone = true

	cfg := testConfig(t)
	cfg.Extra = map[string]interface{}{"channel": "ops"}
	p := New(src, &memSink{}, cfg)
	defer run(t, p)()

	eventually(t, "the pump to park inside GetAudio", func() bool { return src.calls.Load() > 0 })

	p.SetExtra(map[string]interface{}{"channel": "logistics"})
	p.ResetSource()

	eventually(t, "the blocked call to be freed and reopened", func() bool {
		return channelOf(src.lastExtra()) == "logistics"
	})
}

// TestResetSourceWakesTheReconnectDelay: a retune must not cost a full
// reconnect interval of deafness waiting on a timer that is about to be
// irrelevant.
func TestResetSourceWakesTheReconnectDelay(t *testing.T) {
	src := newFakeSource()
	cfg := testConfig(t)
	cfg.Reconnect = 5 * time.Second // far longer than this test will wait
	p := New(src, &memSink{}, cfg)
	defer run(t, p)()

	eventually(t, "the first stream to open", func() bool { return src.calls.Load() > 0 })
	callsBefore := src.calls.Load()

	start := time.Now()
	p.SetExtra(map[string]interface{}{"channel": "logistics"})
	p.ResetSource()

	eventually(t, "the stream to reopen promptly", func() bool { return src.calls.Load() > callsBefore })
	test.That(t, time.Since(start), test.ShouldBeLessThan, 2*time.Second)
}

// TestRapidRetunesConverge: SetExtra is last-writer-wins and the extra is read
// at GetAudio time, i.e. after the cancel, so the loop settles on the final
// value without any debouncing.
func TestRapidRetunesConverge(t *testing.T) {
	src := newFakeSource()
	p := New(src, &memSink{}, testConfig(t))
	defer run(t, p)()

	eventually(t, "the first stream to open", func() bool { return src.calls.Load() > 0 })

	for i := range 50 {
		name := "ch" + string(rune('a'+i%26))
		if i == 49 {
			name = "final"
		}
		p.SetExtra(map[string]interface{}{"channel": name})
		p.ResetSource()
	}

	eventually(t, "the pump to settle on the last channel", func() bool {
		return channelOf(src.lastExtra()) == "final"
	})
	// And it is still a working pump, not a wedged one.
	producedBefore := src.produced.Load()
	eventually(t, "audio to flow on the final channel", func() bool {
		return src.produced.Load() > producedBefore+3
	})
}

func TestResetSinkEndsTheCurrentTransmission(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "a transmission to start", func() bool { return len(sink.snapshot()) > 0 })

	p.ResetSink()

	// The point of ending it is that the floor on the old channel is released
	// rather than held to its hangover, which from in here means the stream
	// really did finish.
	eventually(t, "the transmission to end", func() bool { return sink.endedStreams() > 0 })
}

// --- backoff -----------------------------------------------------------------

// TestBusyBackoffStopsTheSenderSpinning is the fix for a real failure this
// module would otherwise create. A sink that rejects every stream immediately --
// exactly what a busy walkie-talkie channel is -- sends the sender into a tight
// loop opening and losing a PlayStream per chunk, roughly fifty RPCs a second
// for as long as the talk button is held.
func TestBusyBackoffStopsTheSenderSpinning(t *testing.T) {
	src := newFakeSource()
	sink := &rejectingSink{err: errors.New("walkie: channel busy")}

	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	cfg.OnStreamError = func(err error) (time.Duration, string) {
		return 300 * time.Millisecond, "channel busy"
	}
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "the first rejection", func() bool { return sink.calls.Load() > 0 })

	// Chunks arrive every 2ms, so an unbacked-off sender would open hundreds of
	// streams in this window.
	time.Sleep(600 * time.Millisecond)
	test.That(t, sink.calls.Load(), test.ShouldBeLessThanOrEqualTo, uint64(6))

	// The drain loop must be entirely unaffected: backing off is about declining
	// to shout at the sink, not about stopping listening to the microphone.
	stats := p.Stats()
	test.That(t, stats.ChunksIn, test.ShouldBeGreaterThanOrEqualTo, uint64(10))
	test.That(t, stats.Suppressed, test.ShouldNotEqual, 0)

	producedBefore := src.produced.Load()
	eventually(t, "the source to keep producing through a backoff", func() bool {
		return src.produced.Load() > producedBefore+5
	})
}

// TestBackoffExpires: a busy channel becomes free again, and the pump has to
// notice without being prodded.
func TestBackoffExpires(t *testing.T) {
	src := newFakeSource()
	sink := &rejectingSink{err: errors.New("walkie: channel busy")}

	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	cfg.OnStreamError = func(err error) (time.Duration, string) {
		return 100 * time.Millisecond, "channel busy"
	}
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "the first rejection", func() bool { return sink.calls.Load() > 0 })
	after := sink.calls.Load()
	eventually(t, "the sender to try again once the backoff expires", func() bool {
		return sink.calls.Load() > after
	})
}

// TestResetSinkClearsTheBackoff: a backoff earned on one channel says nothing
// about the next one, so retuning must not leave the radio mute.
func TestResetSinkClearsTheBackoff(t *testing.T) {
	p := New(newFakeSource(), &memSink{}, testConfig(t))
	p.blockedUntil.Store(time.Now().Add(time.Hour).UnixNano())

	test.That(t, p.backedOff(), test.ShouldBeTrue)
	p.ResetSink()
	test.That(t, p.backedOff(), test.ShouldBeFalse)
}

// TestUnclassifiedErrorsStillReportVerbatim: OnStreamError gets first refusal,
// but an error it declines to classify must reach the operator unchanged.
func TestUnclassifiedErrorsStillReportVerbatim(t *testing.T) {
	src := newFakeSource()
	sink := &rejectingSink{err: errors.New("speaker on fire")}

	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	cfg.OnStreamError = func(err error) (time.Duration, string) { return 0, "" }
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "the error to be reported", func() bool {
		return p.Stats().LastErr == "speaker on fire"
	})
}

// --- heartbeats --------------------------------------------------------------

// TestHeartbeatsAreInvisibleToEveryOtherCounter is the property that lets the
// hub prime and keep alive a quiet channel for free. A chunk with no audio must
// not touch chunks_in, the peak meter, or silence tracking -- otherwise every
// idle radio would look exactly like a microphone denied by macOS.
func TestHeartbeatsAreInvisibleToEveryOtherCounter(t *testing.T) {
	src := newFakeSource()
	src.chunkFor = func(seq int32) *audioin.AudioChunk {
		// A heartbeat: no audio, but valid AudioInfo.
		return &audioin.AudioChunk{AudioInfo: testFormat.AudioInfo(rutils.CodecPCM16), Sequence: seq}
	}
	sink := &memSink{}

	cfg := testConfig(t)
	cfg.GateMode = GateOpen
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "heartbeats to be seen", func() bool { return p.Stats().LastHeartbeatAge > 0 })
	// A settle window, not a poll: everything below asserts a counter stayed at
	// zero, and no condition can become true to end the wait early.
	time.Sleep(100 * time.Millisecond)

	got := p.Stats()
	test.That(t, got.ChunksIn, test.ShouldEqual, 0)
	test.That(t, got.Transmissions, test.ShouldEqual, 0)
	test.That(t, got.SilentSeconds, test.ShouldEqual, 0)
	test.That(t, got.PeakDBFS, test.ShouldEqual, pcm.SilentDBFS)
	test.That(t, got.LastHeartbeatAge, test.ShouldBeGreaterThan, time.Duration(0))
}
