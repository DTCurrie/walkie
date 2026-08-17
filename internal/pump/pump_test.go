package pump

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/logging"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/audiofmt"
)

const (
	tickInterval = 2 * time.Millisecond
	settle       = 2 * time.Second
)

var testFormat = audiofmt.Format{SampleRateHz: 48000, NumChannels: 1}

// fakeSource mirrors the structure of the real RDK audioin client
// (components/audioin/client.go) on purpose, including its most dangerous
// property: the send into the channel is blocking and is NOT guarded by a
// select on ctx. The cancellation check happens only at the top of the loop, so
// a consumer that stops reading wedges this goroutine permanently and no amount
// of cancelling will free it.
//
// That fidelity is the point. If the pump ever stops draining, `produced` stops
// climbing and the tests below fail.
type fakeSource struct {
	interval time.Duration
	produced atomic.Uint64
	calls    atomic.Uint64
	failures atomic.Int32

	// chunkFor lets a test vary what is emitted, e.g. to inject a bad format.
	chunkFor func(seq int32) *audioin.AudioChunk

	// extraMu guards the record of what each GetAudio call was handed, which is
	// how the retune tests see which channel the pump is listening to.
	extraMu sync.Mutex
	extras  []map[string]interface{}

	// blockUntilDone makes GetAudio never return, mirroring the RDK client's
	// blocking first Recv on a source that has nothing to say yet.
	blockUntilDone bool
}

func (s *fakeSource) recordExtra(extra map[string]interface{}) {
	s.extraMu.Lock()
	defer s.extraMu.Unlock()
	s.extras = append(s.extras, extra)
}

// lastExtra returns the extra map handed to the most recent GetAudio call.
func (s *fakeSource) lastExtra() map[string]interface{} {
	s.extraMu.Lock()
	defer s.extraMu.Unlock()
	if len(s.extras) == 0 {
		return nil
	}
	return s.extras[len(s.extras)-1]
}

func newFakeSource() *fakeSource {
	return &fakeSource{interval: tickInterval}
}

func (s *fakeSource) GetAudio(ctx context.Context, codec string, _ float32, _ int64,
	extra map[string]interface{},
) (chan *audioin.AudioChunk, error) {
	s.calls.Add(1)
	s.recordExtra(extra)
	if s.failures.Load() > 0 {
		s.failures.Add(-1)
		return nil, errors.New("source unavailable")
	}
	if s.blockUntilDone {
		// The RDK client does one blocking Recv before returning, so a source
		// with nothing to say leaves the caller parked in here. Only cancelling
		// the stream context gets it out.
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ch := make(chan *audioin.AudioChunk, 8)
	go func() {
		defer close(ch)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		var seq int32
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			seq++
			var chunk *audioin.AudioChunk
			if s.chunkFor != nil {
				chunk = s.chunkFor(seq)
			} else {
				chunk = pcmChunk(seq, testFormat, codec, 0x20)
			}

			ch <- chunk // blocking, unguarded -- exactly like the RDK client
			s.produced.Add(1)
		}
	}()
	return ch, nil
}

// pcmChunk builds a 20ms chunk whose samples all have the given magnitude.
func pcmChunk(seq int32, f audiofmt.Format, codec string, level byte) *audioin.AudioChunk {
	data := make([]byte, 1920) // 20ms @ 48kHz mono pcm16
	for i := 1; i < len(data); i += 2 {
		data[i] = level // high byte, so level 0 is true digital silence
	}
	return &audioin.AudioChunk{
		AudioData: data,
		AudioInfo: f.AudioInfo(codec),
		Sequence:  seq,
	}
}

type streamRec struct {
	info   rutils.AudioInfo
	chunks int
	bytes  int
	ended  bool
}

// memSink honours the audioout PlayStream contract: drain until the channel is
// closed and then return nil, or return a non-nil error on any early exit.
type memSink struct {
	mu      sync.Mutex
	streams []*streamRec
	delay   time.Duration // simulates a speaker playing in real time
}

func (s *memSink) PlayStream(ctx context.Context, info *rutils.AudioInfo,
	chunks <-chan []byte, _ map[string]interface{},
) error {
	rec := &streamRec{}
	if info != nil {
		rec.info = *info
	}
	s.mu.Lock()
	s.streams = append(s.streams, rec)
	s.mu.Unlock()

	end := func() {
		s.mu.Lock()
		rec.ended = true
		s.mu.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			end()
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				end()
				return nil
			}
			if s.delay > 0 {
				select {
				case <-ctx.Done():
					end()
					return ctx.Err()
				case <-time.After(s.delay):
				}
			}
			s.mu.Lock()
			rec.chunks++
			rec.bytes += len(chunk)
			s.mu.Unlock()
		}
	}
}

// rejectingSink reproduces what an audio_out that does not serve PlayStream
// looks like from in here. The RDK client accepts the first chunk, discovers on
// Send that the server hung up, and returns -- after which nothing reads the
// chunks channel ever again (components/audioout/client.go:125). A pump that
// assumes someone is still reading blocks on its next send forever.
type rejectingSink struct {
	calls atomic.Uint64
	err   error
}

func (s *rejectingSink) PlayStream(ctx context.Context, _ *rutils.AudioInfo,
	chunks <-chan []byte, _ map[string]interface{},
) error {
	s.calls.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-chunks:
		return s.err
	}
}

// endedStreams counts PlayStream calls that have returned.
func (s *memSink) endedStreams() int {
	n := 0
	for _, r := range s.snapshot() {
		if r.ended {
			n++
		}
	}
	return n
}

func (s *memSink) snapshot() []streamRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]streamRec, 0, len(s.streams))
	for _, r := range s.streams {
		out = append(out, *r)
	}
	return out
}

func (s *memSink) totalChunks() int {
	total := 0
	for _, r := range s.snapshot() {
		total += r.chunks
	}
	return total
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Reconnect: 10 * time.Millisecond,
		MaxQueued: 4,
		Logger:    logging.NewTestLogger(t),
	}
}

// run starts a pump and returns a stop function that waits for it to unwind.
func run(t *testing.T, p *Pump) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(settle):
			t.Error("Run did not return after cancellation")
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestDrainsWhileGateClosed is the single most important test in this package.
//
// The RDK audioin client pushes into a capacity-8 channel with a blocking send
// that is not guarded by a context select. If the pump treats a closed gate as a
// reason to stop reading, that goroutine wedges permanently, the source's
// stream never unwinds, and the microphone is effectively stuck until the
// process restarts. Muted must still mean drained.
func TestDrainsWhileGateClosed(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	p := New(src, sink, testConfig(t)) // GateManual, not talking
	defer run(t, p)()

	eventually(t, "the stream to start", func() bool { return p.Stats().ChunksIn > 0 })

	before := p.Stats().ChunksIn
	producedBefore := src.produced.Load()

	eventually(t, "chunks to keep arriving while the gate is shut", func() bool {
		return p.Stats().ChunksIn > before+5
	})
	eventually(t, "the source goroutine to keep producing", func() bool {
		return src.produced.Load() > producedBefore+5
	})

	after := p.Stats()
	test.That(t, after.ChunksOut, test.ShouldEqual, 0)
	test.That(t, sink.totalChunks(), test.ShouldEqual, 0)
	test.That(t, after.GateOpen, test.ShouldBeFalse)
}

func TestForwardsWhileTalking(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	eventually(t, "the stream to start", func() bool { return p.Stats().ChunksIn > 0 })
	p.SetTalking(true)

	eventually(t, "audio to reach the sink", func() bool { return sink.totalChunks() > 3 })

	streams := sink.snapshot()
	test.That(t, len(streams), test.ShouldNotEqual, 0)
	test.That(t, streams[0].info, test.ShouldResemble, rutils.AudioInfo{
		Codec: rutils.CodecPCM16, SampleRateHz: 48000, NumChannels: 1,
	})
	s := p.Stats()
	test.That(t, s.GateOpen, test.ShouldBeTrue)
	test.That(t, s.ChunksOut, test.ShouldBeGreaterThan, uint64(0))
}

// TestGateTogglesTransmissions: closing the gate must end the transmission, and
// reopening it must start a new one rather than resuming the old stream.
func TestGateTogglesTransmissions(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	eventually(t, "the stream to start", func() bool { return p.Stats().ChunksIn > 0 })

	p.SetTalking(true)
	eventually(t, "the first transmission", func() bool { return sink.totalChunks() > 2 })
	p.SetTalking(false)

	// The transmission closes only once the queue has been idle past
	// endOfTransmissionIdle, so a brief gate blip deliberately does not tear the
	// stream down. Wait for PlayStream to actually return before reopening.
	eventually(t, "the first transmission to close", func() bool {
		return sink.endedStreams() == 1
	})
	test.That(t, len(sink.snapshot()), test.ShouldEqual, 1)
	firstTotal := sink.totalChunks()

	// Check both conditions together: a stream is recorded the moment PlayStream
	// is entered, which is before any audio has reached it.
	p.SetTalking(true)
	eventually(t, "a second transmission carrying audio", func() bool {
		return len(sink.snapshot()) == 2 && sink.totalChunks() > firstTotal
	})
}

// TestSinkRejectingPlayStreamDoesNotWedgeSender: A chunk we cannot interpret
// must be counted and skipped, never fatal to the stream, because a mid-stream
// error cannot be reported to the caller anyway.
// TestSinkRejectingPlayStreamDoesNotWedgeSender covers the failure seen against
// viam:system-audio 0.0.6, which serves Play but not PlayStream and so answers
// with a gRPC Unimplemented. The stream dying is not the interesting part --
// the sender surviving it is. Before this was handled, the send after the
// rejection blocked forever and the pump silently stopped trying.
func TestSinkRejectingPlayStreamDoesNotWedgeSender(t *testing.T) {
	src := newFakeSource()
	sink := &rejectingSink{err: status.Error(codes.Unimplemented, "")}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	p.SetTalking(true)

	// More than one attempt proves transmit returned rather than blocking on a
	// channel nobody reads.
	eventually(t, "the sender to retry after a rejected stream", func() bool {
		return sink.calls.Load() > 1
	})

	// The drain loop is upstream of all of this and must be untouched by it.
	before := p.Stats().ChunksIn
	eventually(t, "the source to keep being drained", func() bool {
		return p.Stats().ChunksIn > before
	})

	// The raw error crosses three client/server hops and arrives as a bare
	// "code = Unimplemented desc =" nested three deep, so last_error carries the
	// explanation instead.
	test.That(t, p.Stats().LastErr, test.ShouldContainSubstring, "does not implement PlayStream")
}

// TestSinkErrorsOtherThanUnimplementedAreReportedVerbatim keeps the translation
// above narrow: only Unimplemented means "wrong module version".
func TestSinkErrorsOtherThanUnimplementedAreReportedVerbatim(t *testing.T) {
	src := newFakeSource()
	sink := &rejectingSink{err: errors.New("speaker device disappeared")}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	p.SetTalking(true)
	eventually(t, "the error to surface", func() bool { return p.Stats().LastErr != "" })

	test.That(t, p.Stats().LastErr, test.ShouldContainSubstring, "speaker device disappeared")
}

func TestBadChunksAreSkippedNotFatal(t *testing.T) {
	src := newFakeSource()
	src.chunkFor = func(seq int32) *audioin.AudioChunk {
		switch seq % 3 {
		case 0:
			// Nil AudioInfo is legal on the wire and must not panic.
			c := pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x20)
			c.AudioInfo = nil
			return c
		case 1:
			// A codec this module cannot carry.
			return pcmChunk(seq, testFormat, rutils.CodecMP3, 0x20)
		default:
			return pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x20)
		}
	}
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	p.SetTalking(true)
	defer run(t, p)()

	eventually(t, "good chunks to reach the sink", func() bool { return sink.totalChunks() > 3 })
	eventually(t, "bad chunks to be counted", func() bool { return p.Stats().FormatMismatch > 3 })

	// And the stream must still be alive and flowing.
	before := p.Stats().ChunksIn
	eventually(t, "the stream to keep flowing after a bad chunk", func() bool {
		return p.Stats().ChunksIn > before
	})
}

// TestDropsOldestUnderBackpressure: A slow sink must cost audio quality, not
// ever-growing latency.
func TestDropsOldestUnderBackpressure(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{delay: 40 * time.Millisecond} // far slower than the 2ms source
	p := New(src, sink, testConfig(t))
	p.SetTalking(true)
	defer run(t, p)()

	eventually(t, "drops to accumulate", func() bool { return p.Stats().ChunksDropped > 5 })

	s := p.Stats()
	test.That(t, s.ChunksIn, test.ShouldBeGreaterThan, s.ChunksOut)
	// The source must still be running at full tilt despite the slow sink.
	before := src.produced.Load()
	eventually(t, "the source to keep running despite the slow sink", func() bool {
		return src.produced.Load() > before+5
	})
}

func TestReconnectsAfterSourceFailure(t *testing.T) {
	src := newFakeSource()
	src.failures.Store(2)
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	eventually(t, "the source to be retried", func() bool { return src.calls.Load() >= 3 })
	eventually(t, "the stream to recover", func() bool {
		s := p.Stats()
		return s.Connected && s.ChunksIn > 0
	})
	test.That(t, p.Stats().Reconnects, test.ShouldBeGreaterThan, uint64(0))
}

// TestFormatChangeOpensNewStream: A format change mid-stream must open a fresh
// PlayStream, because PlayStreamInit fixes the AudioInfo for the life of a
// stream.
func TestFormatChangeOpensNewStream(t *testing.T) {
	src := newFakeSource()
	stereo := audiofmt.Format{SampleRateHz: 48000, NumChannels: 2}
	src.chunkFor = func(seq int32) *audioin.AudioChunk {
		if seq > 5 {
			return pcmChunk(seq, stereo, rutils.CodecPCM16, 0x20)
		}
		return pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x20)
	}
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	p.SetTalking(true)
	defer run(t, p)()

	eventually(t, "a second stream for the new format", func() bool {
		return len(sink.snapshot()) >= 2
	})

	streams := sink.snapshot()
	test.That(t, streams[0].info.NumChannels, test.ShouldEqual, 1)
	last := streams[len(streams)-1].info
	test.That(t, last.NumChannels, test.ShouldEqual, 2)
}

func TestVoxGate(t *testing.T) {
	loud := true
	var mu sync.Mutex
	src := newFakeSource()
	src.chunkFor = func(seq int32) *audioin.AudioChunk {
		mu.Lock()
		defer mu.Unlock()
		if loud {
			return pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x40)
		}
		return pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x00) // silence
	}

	cfg := testConfig(t)
	cfg.GateMode = GateVox
	cfg.VoxThreshold = -30
	cfg.VoxHangover = 50 * time.Millisecond

	sink := &memSink{}
	p := New(src, sink, cfg)
	defer run(t, p)()

	eventually(t, "vox to open on loud audio", func() bool { return sink.totalChunks() > 2 })

	mu.Lock()
	loud = false
	mu.Unlock()

	eventually(t, "vox to close after the hangover", func() bool { return !p.Stats().GateOpen })

	// A settle window, not a poll: proving nothing more arrives means waiting,
	// since no condition can become true to end the wait early.
	settled := sink.totalChunks()
	time.Sleep(100 * time.Millisecond)
	test.That(t, sink.totalChunks(), test.ShouldEqual, settled)
	// Silence must still be drained and counted.
	before := p.Stats().ChunksIn
	eventually(t, "silence to still be drained and counted", func() bool {
		return p.Stats().ChunksIn > before
	})
}

func TestSetTalkingForExpires(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	eventually(t, "the stream to start", func() bool { return p.Stats().ChunksIn > 0 })
	p.SetTalkingFor(60 * time.Millisecond)

	eventually(t, "audio to flow", func() bool { return sink.totalChunks() > 0 })
	eventually(t, "the deadline to close the gate", func() bool { return !p.Talking() })

	// A settle window, not a poll: proving nothing more arrives means waiting,
	// since no condition can become true to end the wait early.
	settled := sink.totalChunks()
	time.Sleep(100 * time.Millisecond)
	test.That(t, sink.totalChunks(), test.ShouldEqual, settled)
}

// TestDigitalSilenceIsVisible: A silently-denied macOS microphone delivers a
// well-formed stream of zeros. The peak level is what makes that visible, so it
// must not be masked.
func TestDigitalSilenceIsVisible(t *testing.T) {
	src := newFakeSource()
	src.chunkFor = func(seq int32) *audioin.AudioChunk {
		return pcmChunk(seq, testFormat, rutils.CodecPCM16, 0x00)
	}
	sink := &memSink{}
	p := New(src, sink, testConfig(t))
	defer run(t, p)()

	eventually(t, "silence to be observed", func() bool {
		s := p.Stats()
		return s.ChunksIn > 3 && s.PeakDBFS == audiofmt.SilentDBFS && s.SilentSeconds > 0
	})
}

// TestSilentSecondsMeasuresTheWholeSilence: silent_seconds must report the full
// run length, not the time since the last warning. trackSilence rate-limits its
// warning by re-arming a clock, and when that clock was the same field Stats
// reads, the number saw-toothed between 0 and silentWarnAfter -- so a
// microphone dead for a minute reported about two seconds, which is exactly the
// case the field exists to expose.
func TestSilentSecondsMeasuresTheWholeSilence(t *testing.T) {
	p := New(newFakeSource(), &memSink{}, testConfig(t))

	start := time.Unix(1_700_000_000, 0)
	p.trackSilence(audiofmt.SilentDBFS, start)

	// Well past several warning intervals.
	for elapsed := time.Second; elapsed <= time.Minute; elapsed += time.Second {
		p.trackSilence(audiofmt.SilentDBFS, start.Add(elapsed))
	}

	onset := time.Unix(0, p.silentSince.Load())
	test.That(t, start.Add(time.Minute).Sub(onset), test.ShouldEqual, time.Minute)

	// Any real audio clears both clocks, so the next silence starts fresh.
	p.trackSilence(-20, start.Add(time.Minute+time.Second))
	test.That(t, p.silentSince.Load(), test.ShouldEqual, int64(0))
	test.That(t, p.silentWarnAt.Load(), test.ShouldEqual, int64(0))
}

func TestStatsSnapshot(t *testing.T) {
	src := newFakeSource()
	sink := &memSink{}
	p := New(src, sink, testConfig(t))

	// Before Run, the snapshot must be sane rather than a zero-value trap.
	s := p.Stats()
	test.That(t, s.Connected, test.ShouldBeFalse)
	test.That(t, s.GateMode, test.ShouldEqual, "manual")
	test.That(t, s.PeakDBFS, test.ShouldEqual, audiofmt.SilentDBFS)
	test.That(t, s.LastChunkAge, test.ShouldEqual, 0)

	p.SetGateMode(GateOpen)
	test.That(t, p.Stats().GateMode, test.ShouldEqual, "open")
}

func TestParseGateMode(t *testing.T) {
	for in, want := range map[string]GateMode{
		"":        GateManual,
		"manual":  GateManual,
		"  VOX  ": GateVox,
		"vox":     GateVox,
		"open":    GateOpen,
		"always":  GateOpen,
	} {
		got, err := ParseGateMode(in)
		test.That(t, err, test.ShouldBeNil)
		test.That(t, got, test.ShouldEqual, want)
	}
	_, err := ParseGateMode("sometimes")
	test.That(t, err, test.ShouldNotBeNil)
}
