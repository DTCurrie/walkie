package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

var testFormat = audiofmt.Format{SampleRateHz: 48000, NumChannels: 1}

// testHangover is the shortest hangover the bus will accept. Tests that wait it
// out pay for it in wall clock, which is preferable to letting the bound be
// configurable down to something that would not survive contact with a real
// talker's 400ms stream teardown.
const testHangover = MinFloorHangover

func testConfig(t *testing.T, channels ...ChannelConfig) Config {
	t.Helper()
	if len(channels) == 0 {
		channels = []ChannelConfig{{Name: "ops"}}
	}
	return Config{
		Channels: channels,
		// Deep enough that a test asserting an exact delivery count is not
		// racing the drop-oldest queue. Tests about dropping set their own.
		MaxQueued:     16,
		FloorHangover: testHangover,
		FloorIdle:     200 * time.Millisecond,
		Keepalive:     50 * time.Millisecond,
		DefaultFormat: testFormat,
		Logger:        logging.NewTestLogger(t),
	}
}

func newTestBus(t *testing.T, cfg Config) *Bus {
	t.Helper()
	b, err := New(cfg)
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// run starts the bus watchdog for the life of the test.
func run(t *testing.T, b *Bus) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func pcm(n int) []byte { return make([]byte, n*2) }

func publish(t *testing.T, b *Bus, channel, member string) *Tx {
	t.Helper()
	tx, err := b.Publish(context.Background(), TxReq{Channel: channel, Member: member, Format: testFormat})
	test.That(t, err, test.ShouldBeNil)
	return tx
}

// listener drains a subscription in the background, mirroring what the RDK's
// audioin server does with the channel it is handed.
type listener struct {
	out    chan *audioin.AudioChunk
	cancel func()

	mu     sync.Mutex
	chunks []*audioin.AudioChunk
	closed bool
}

func subscribe(t *testing.T, b *Bus, ctx context.Context, channel, member string) *listener {
	t.Helper()
	out, cancel, err := b.Subscribe(ctx, SubReq{Channel: channel, Member: member})
	test.That(t, err, test.ShouldBeNil)
	l := &listener{out: out, cancel: cancel}
	go func() {
		for c := range out {
			l.mu.Lock()
			l.chunks = append(l.chunks, c)
			l.mu.Unlock()
		}
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
	}()
	t.Cleanup(cancel)
	return l
}

// audible returns only the chunks carrying real audio, i.e. everything that is
// not a primer or a keepalive.
func (l *listener) audible() []*audioin.AudioChunk {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*audioin.AudioChunk
	for _, c := range l.chunks {
		if len(c.AudioData) > 0 {
			out = append(out, c)
		}
	}
	return out
}

// heartbeats counts the chunks carrying no audio: the primer and the keepalives.
func (l *listener) heartbeats() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.chunks {
		if len(c.AudioData) == 0 {
			n++
		}
	}
	return n
}

func (l *listener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.chunks)
}

func (l *listener) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// --- fan-out -----------------------------------------------------------------

func TestFanOutReachesEverySubscriber(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx := context.Background()

	a := subscribe(t, b, ctx, "ops", "alpha")
	c := subscribe(t, b, ctx, "ops", "charlie")

	tx := publish(t, b, "ops", "bravo")
	defer tx.Close()
	for range 5 {
		test.That(t, tx.Send(pcm(32)), test.ShouldBeNil)
	}

	eventually(t, "alpha to hear all five", func() bool { return len(a.audible()) == 5 })
	eventually(t, "charlie to hear all five", func() bool { return len(c.audible()) == 5 })
}

// TestSelfEchoSuppressed defends the one rule that keeps a member's own voice
// out of their own speaker. Without it, every radio is an acoustic feedback
// loop by construction rather than by accident.
func TestSelfEchoSuppressed(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx := context.Background()

	alpha := subscribe(t, b, ctx, "ops", "alpha")
	bravo := subscribe(t, b, ctx, "ops", "bravo")

	tx := publish(t, b, "ops", "alpha")
	defer tx.Close()
	for range 4 {
		_ = tx.Send(pcm(32))
	}

	eventually(t, "bravo to hear alpha", func() bool { return len(bravo.audible()) == 4 })
	test.That(t, len(alpha.audible()), test.ShouldEqual, 0)
}

// TestSlowSubscriberDoesNotBlockTalker is the property the whole fan-out rests
// on. One listener that never reads must not be able to stall a talker or
// starve the listeners next to it.
func TestSlowSubscriberDoesNotBlockTalker(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx := context.Background()

	// Subscribed, and deliberately never drained.
	_, _, err := b.Subscribe(ctx, SubReq{Channel: "ops", Member: "dead"})
	test.That(t, err, test.ShouldBeNil)
	live := subscribe(t, b, ctx, "ops", "live")

	tx := publish(t, b, "ops", "talker")
	defer tx.Close()

	start := time.Now()
	for range 10_000 {
		test.That(t, tx.Send(pcm(16)), test.ShouldBeNil)
	}
	test.That(t, time.Since(start), test.ShouldBeLessThan, 5*time.Second)

	eventually(t, "the live listener to keep receiving", func() bool { return len(live.audible()) > 100 })
}

func TestDeadSubscriberDropsOldest(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxQueued = 4
	cfg.Keepalive = 0
	b := newTestBus(t, cfg)

	out, cancel, err := b.Subscribe(context.Background(), SubReq{Channel: "ops", Member: "dead"})
	test.That(t, err, test.ShouldBeNil)
	defer cancel()

	tx := publish(t, b, "ops", "talker")
	defer tx.Close()
	for range 100 {
		_ = tx.Send(pcm(8))
	}

	eventually(t, "drops to be counted", func() bool {
		for _, cs := range b.Stats().Channels {
			if cs.Name == "ops" && cs.ChunksDropped > 0 {
				return true
			}
		}
		return false
	})

	// The queue is bounded, so the backlog can never be more than the primer
	// sitting in out plus one full queue.
	test.That(t, len(out), test.ShouldBeLessThanOrEqualTo, 1)
}

// TestSubscribeDuringFanoutIsSafe exercises the copy-on-write subscriber list
// against a talker. Run under -race; the assertion is that nothing tears.
func TestSubscribeDuringFanoutIsSafe(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx := publish(t, b, "ops", "talker")
	defer tx.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				out, unsub, err := b.Subscribe(ctx, SubReq{
					Channel: "ops", Member: fmt.Sprintf("m%d-%d", i, n),
				})
				if err != nil {
					return
				}
				go func() {
					for range out {
					}
				}()
				unsub()
			}
		}()
	}

	for range 10_000 {
		_ = tx.Send(pcm(8))
	}
	close(stop)
	wg.Wait()
}

// --- subscriber lifecycle ----------------------------------------------------

// TestWriterExitsWhenServerStopsReading covers the RDK's actual behaviour: the
// audioin server reads inside a select on the stream context and simply stops
// reading when that context ends, without draining. A plain send in the writer
// would leak a goroutine for every listener that hangs up.
func TestWriterExitsWhenServerStopsReading(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx, cancel := context.WithCancel(context.Background())

	out, _, err := b.Subscribe(ctx, SubReq{Channel: "ops", Member: "gone"})
	test.That(t, err, test.ShouldBeNil)

	// Wedge the writer: out is cap 1 and holds the primer, and the queue fills
	// behind it, so the writer is parked inside emit.
	tx := publish(t, b, "ops", "talker")
	for range 50 {
		_ = tx.Send(pcm(8))
	}
	tx.Close()

	cancel() // the listener hangs up without draining a thing

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for range out {
		}
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the writer goroutine did not exit after its context ended")
	}
}

func TestSubscriberCancelUnregisters(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx := context.Background()

	l := subscribe(t, b, ctx, "ops", "alpha")
	eventually(t, "the listener to register", func() bool { return b.Stats().Channels[0].Listeners == 1 })

	l.cancel()
	eventually(t, "the listener to deregister", func() bool { return b.Stats().Channels[0].Listeners == 0 })
	eventually(t, "the out channel to close", l.isClosed)

	// Idempotent: a double hang-up must not panic on a closed channel.
	l.cancel()
	l.cancel()
}

func TestDurationSecondsClosesSubscription(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	out, cancel, err := b.Subscribe(context.Background(), SubReq{
		Channel: "ops", Member: "collector", Duration: 100 * time.Millisecond,
	})
	test.That(t, err, test.ShouldBeNil)
	defer cancel()

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for range out {
		}
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("a subscription with a duration never ended; the data collector would hang")
	}
}

// TestNeverEmitsNilChunkOrNilAudioInfo guards two unchecked dereferences in the
// RDK. audioin's client reads resp.Audio.AudioInfo without checking resp.Audio,
// and its collector reads chunk.AudioInfo.SampleRateHz without a nil check
// whenever its buffer is empty -- which a zero-length chunk guarantees. Either
// would panic viam-server on the listening machine, not here.
func TestNeverEmitsNilChunkOrNilAudioInfo(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	ctx := context.Background()

	l := subscribe(t, b, ctx, "ops", "alpha")

	tx := publish(t, b, "ops", "bravo")
	for range 3 {
		_ = tx.Send(pcm(16))
	}
	tx.Close()

	eventually(t, "audio to arrive", func() bool { return len(l.audible()) >= 3 })
	// Keepalives too, since they are the chunks most likely to carry a nil.
	eventually(t, "a keepalive to arrive", func() bool { return l.heartbeats() >= 1 })

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.chunks {
		test.That(t, c, test.ShouldNotBeNil)
		test.That(t, c.AudioInfo, test.ShouldNotBeNil)
	}
}

// TestPrimerPrecedesAnyAudio covers the reason the primer exists at all: the
// RDK's audioin client blocks on one Recv before GetAudio returns, so a channel
// where nobody is talking would leave a listener stuck inside GetAudio and
// reporting itself disconnected.
func TestPrimerPrecedesAnyAudio(t *testing.T) {
	cfg := testConfig(t)
	cfg.Keepalive = 0
	b := newTestBus(t, cfg)

	out, cancel, err := b.Subscribe(context.Background(), SubReq{Channel: "ops", Member: "alpha"})
	test.That(t, err, test.ShouldBeNil)
	defer cancel()

	select {
	case first := <-out:
		test.That(t, first, test.ShouldNotBeNil)
		test.That(t, len(first.AudioData), test.ShouldEqual, 0)
		test.That(t, first.AudioInfo, test.ShouldNotBeNil)
	case <-time.After(time.Second):
		t.Fatal("no primer arrived; a listener would block inside GetAudio on a quiet channel")
	}
}

func TestKeepaliveEmitsHeartbeats(t *testing.T) {
	b := newTestBus(t, testConfig(t)) // 50ms keepalive
	l := subscribe(t, b, context.Background(), "ops", "alpha")

	eventually(t, "keepalives to arrive", func() bool { return l.count() >= 3 })
	test.That(t, len(l.audible()), test.ShouldEqual, 0)
}

// --- floor control -----------------------------------------------------------

func TestFirstTalkerWins(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	first := publish(t, b, "ops", "alpha")
	defer first.Close()

	_, err := b.Publish(context.Background(), TxReq{Channel: "ops", Member: "bravo", Format: testFormat})
	test.That(t, IsBusy(err), test.ShouldBeTrue)

	// The first talker is entirely unaffected.
	test.That(t, first.Send(pcm(8)), test.ShouldBeNil)
}

func TestConcurrentAcquireHasExactlyOneWinner(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	var (
		mu      sync.Mutex
		winners []*Tx
		wg      sync.WaitGroup
	)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := b.Publish(context.Background(), TxReq{
				Channel: "ops", Member: fmt.Sprintf("m%d", i), Format: testFormat,
			})
			if err == nil {
				mu.Lock()
				winners = append(winners, tx)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	test.That(t, len(winners), test.ShouldEqual, 1)
}

func TestHangoverReservesFloorForSameMember(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	tx := publish(t, b, "ops", "alpha")
	tx.Close()

	// Still inside the hangover: reserved to alpha, refused to anyone else.
	_, err := b.Publish(context.Background(), TxReq{Channel: "ops", Member: "bravo", Format: testFormat})
	test.That(t, IsBusy(err), test.ShouldBeTrue)
	again := publish(t, b, "ops", "alpha")
	again.Close()
}

func TestFloorReleasedAfterHangover(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	tx := publish(t, b, "ops", "alpha")
	tx.Close()

	// Polled rather than slept out, so this asserts the floor frees within a
	// bound instead of at one exact instant.
	var next *Tx
	eventually(t, "the floor to free for another member", func() bool {
		got, err := b.Publish(context.Background(),
			TxReq{Channel: "ops", Member: "bravo", Format: testFormat})
		if err != nil {
			return false
		}
		next = got
		return true
	})
	next.Close()
}

// TestSameMemberReacquireSupersedes: a member must never be locked out of a
// channel they already hold. A format change tears one transmission down and
// opens another, and a network blip can leave a stream we think is live but is
// not.
func TestSameMemberReacquireSupersedes(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	first := publish(t, b, "ops", "alpha")
	second := publish(t, b, "ops", "alpha")

	select {
	case <-first.Revoked():
	case <-time.After(time.Second):
		t.Fatal("the superseded transmission was never revoked")
	}
	test.That(t, second.Send(pcm(8)), test.ShouldBeNil)
	second.Close()
}

func TestRevokedTxCannotSend(t *testing.T) {
	b := newTestBus(t, testConfig(t))
	l := subscribe(t, b, context.Background(), "ops", "listener")

	tx := publish(t, b, "ops", "alpha")
	_, err := b.ClearFloor("ops")
	test.That(t, err, test.ShouldBeNil)

	test.That(t, tx.Send(pcm(8)), test.ShouldWrap, ErrRevoked)
	// A settle window, not a poll: the assertion is that nothing arrives.
	time.Sleep(50 * time.Millisecond)
	test.That(t, l.audible(), test.ShouldBeEmpty)
}

// TestLateCloseDoesNotStealNewHolderFloor is the identity guard. Revocation
// frees the floor immediately so a new talker can take it at once, which means
// the revoked handler's deferred Close runs while somebody else legitimately
// holds it. Comparing anything but pointer identity loses the channel here.
func TestLateCloseDoesNotStealNewHolderFloor(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	alpha := publish(t, b, "ops", "alpha")
	_, err := b.ClearFloor("ops")
	test.That(t, err, test.ShouldBeNil)

	bravo := publish(t, b, "ops", "bravo")
	defer bravo.Close()

	// Alpha's handler finally unwinds and runs its deferred Close.
	alpha.Close()

	test.That(t, b.Stats().Channels[0].Holder, test.ShouldEqual, "bravo")
	_, pubErr := b.Publish(context.Background(), TxReq{Channel: "ops", Member: "charlie", Format: testFormat})
	test.That(t, IsBusy(pubErr), test.ShouldBeTrue)
}

// TestWatchdogReclaimsFloorFromSilentTalker covers the power-cut case. The viam
// rpc server sets no keepalive ServerParameters, so a member that loses power
// leaves its stream context live here for minutes; without the watchdog its
// floor outlives it by just as long.
func TestWatchdogReclaimsFloorFromSilentTalker(t *testing.T) {
	b := newTestBus(t, testConfig(t)) // FloorIdle 200ms
	run(t, b)

	tx := publish(t, b, "ops", "ghost")
	defer tx.Close()

	select {
	case <-tx.Revoked():
	case <-time.After(3 * time.Second):
		t.Fatal("the watchdog never reclaimed the floor from a silent talker")
	}
	eventually(t, "the channel to read as free", func() bool { return b.Stats().Channels[0].Holder == "" })

	// And it is genuinely free, not merely reserved to the corpse.
	next := publish(t, b, "ops", "bravo")
	next.Close()
}

// --- errors and formats ------------------------------------------------------

// TestBusyCodeSurvivesTripleWrapping mirrors the trip a real error takes: hub
// module to hub server to member server to member module, wrapped at each hop.
func TestBusyCodeSurvivesTripleWrapping(t *testing.T) {
	err := ToStatus(BusyError("alpha"))
	for range 3 {
		err = fmt.Errorf("wrapped: %w", err)
	}
	test.That(t, IsBusy(err), test.ShouldBeTrue)
	test.That(t, status.Code(err), test.ShouldEqual, codes.FailedPrecondition)

	// And it must stay distinguishable from a hub that is simply down.
	test.That(t, IsBusy(status.Error(codes.Unavailable, "connection refused")), test.ShouldBeFalse)
}

func TestUnknownChannelIsNotFound(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	_, _, err := b.Subscribe(context.Background(), SubReq{Channel: "nope", Member: "alpha"})
	test.That(t, errors.Is(err, ErrUnknownChannel), test.ShouldBeTrue)
	test.That(t, status.Code(ToStatus(err)), test.ShouldEqual, codes.NotFound)
	// The error names what is actually on offer, so the fix is obvious.
	test.That(t, err.Error(), test.ShouldContainSubstring, "ops")

	_, pubErr := b.Publish(context.Background(), TxReq{Channel: "nope", Member: "alpha", Format: testFormat})
	test.That(t, pubErr, test.ShouldWrap, ErrUnknownChannel)
}

func TestChannelFormatMismatchRejected(t *testing.T) {
	cfg := testConfig(t, ChannelConfig{Name: "ops", SampleRate: 16000, NumChannels: 1})
	b := newTestBus(t, cfg)

	_, err := b.Publish(context.Background(), TxReq{
		Channel: "ops", Member: "alpha",
		Format: audiofmt.Format{SampleRateHz: 48000, NumChannels: 1},
	})
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, strings.Contains(err.Error(), "16000"), test.ShouldBeTrue)

	// A talker in the declared format is accepted.
	tx, err := b.Publish(context.Background(), TxReq{
		Channel: "ops", Member: "alpha",
		Format: audiofmt.Format{SampleRateHz: 16000, NumChannels: 1},
	})
	test.That(t, err, test.ShouldBeNil)
	tx.Close()
}

func TestNonPCM16SubscriberRejected(t *testing.T) {
	b := newTestBus(t, testConfig(t))

	_, _, err := b.Subscribe(context.Background(), SubReq{Channel: "ops", Member: "alpha", Codec: rutils.CodecOpus})
	test.That(t, err, test.ShouldNotBeNil)
}

func TestRejectsBadConfig(t *testing.T) {
	base := func() Config { return testConfig(t) }

	t.Run("no channels", func(t *testing.T) {
		cfg := base()
		cfg.Channels = nil
		_, err := New(cfg)
		test.That(t, err, test.ShouldNotBeNil)
	})
	t.Run("duplicate channel", func(t *testing.T) {
		cfg := base()
		cfg.Channels = []ChannelConfig{{Name: "ops"}, {Name: "ops"}}
		_, err := New(cfg)
		test.That(t, err, test.ShouldNotBeNil)
	})
	t.Run("hangover shorter than a stream teardown", func(t *testing.T) {
		cfg := base()
		cfg.FloorHangover = 10 * time.Millisecond
		_, err := New(cfg)
		test.That(t, err, test.ShouldNotBeNil)
	})
	t.Run("half a format", func(t *testing.T) {
		cfg := base()
		cfg.Channels = []ChannelConfig{{Name: "ops", SampleRate: 16000}}
		_, err := New(cfg)
		test.That(t, err, test.ShouldNotBeNil)
	})
}

// --- shutdown ----------------------------------------------------------------

// TestCloseWithLiveSubscribersAndHeldFloor: shutting down must be prompt, must
// revoke the floor, and must end every listener's stream cleanly rather than
// with an error, so members reconnect quietly.
func TestCloseWithLiveSubscribersAndHeldFloor(t *testing.T) {
	b, err := New(testConfig(t))
	test.That(t, err, test.ShouldBeNil)

	ctx := context.Background()
	a := subscribe(t, b, ctx, "ops", "alpha")
	c := subscribe(t, b, ctx, "ops", "charlie")
	tx := publish(t, b, "ops", "bravo")

	start := time.Now()
	test.That(t, b.Close(), test.ShouldBeNil)
	test.That(t, time.Since(start), test.ShouldBeLessThan, time.Second)

	select {
	case <-tx.Revoked():
	case <-time.After(time.Second):
		t.Fatal("Close did not revoke the held floor")
	}
	eventually(t, "alpha's stream to end", a.isClosed)
	eventually(t, "charlie's stream to end", c.isClosed)

	// Idempotent.
	test.That(t, b.Close(), test.ShouldBeNil)
}

func TestCallsAfterCloseAreUnavailable(t *testing.T) {
	b, err := New(testConfig(t))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, b.Close(), test.ShouldBeNil)

	_, _, subErr := b.Subscribe(context.Background(), SubReq{Channel: "ops", Member: "alpha"})
	test.That(t, errors.Is(subErr, ErrClosed), test.ShouldBeTrue)
	_, pubErr := b.Publish(context.Background(), TxReq{Channel: "ops", Member: "alpha", Format: testFormat})
	test.That(t, errors.Is(pubErr, ErrClosed), test.ShouldBeTrue)

	// Unavailable, because that is what it is: the machine is mid-rebuild and
	// will be back. This is exactly the distinction ErrBusy must not blur.
	test.That(t, status.Code(ToStatus(subErr)), test.ShouldEqual, codes.Unavailable)
}
