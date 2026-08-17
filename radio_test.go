package walkie

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/rdk/testutils/inject"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/bus"
)

// hubEndpoints builds an uplink and downlink over a real bus, so radio tests
// exercise the actual routing rather than a mock of it.
func hubEndpoints(t *testing.T, channels ...string) (audioout.AudioOut, audioin.AudioIn) {
	t.Helper()
	deps := depsWithBus(newTestBusResource(t, channels...))
	return newTestUplink(t, deps, &UplinkConfig{Bus: "bus"}),
		newTestDownlink(t, deps, &DownlinkConfig{Bus: "bus"})
}

// localMic emits chunks forever, like a real microphone.
func localMic(t *testing.T) *inject.AudioIn {
	t.Helper()
	mic := inject.NewAudioIn("mic")
	mic.GetAudioFunc = func(ctx context.Context, codec string, _ float32, _ int64,
		_ map[string]interface{},
	) (chan *audioin.AudioChunk, error) {
		ch := make(chan *audioin.AudioChunk, 8)
		go func() {
			defer close(ch)
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			var seq int32
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				seq++
				data := make([]byte, 320)
				for i := 1; i < len(data); i += 2 {
					data[i] = 0x20
				}
				ch <- &audioin.AudioChunk{
					AudioData: data,
					AudioInfo: &rutils.AudioInfo{
						Codec: rutils.CodecPCM16, SampleRateHz: 16000, NumChannels: 1,
					},
					Sequence: seq,
				}
			}
		}()
		return ch, nil
	}
	return mic
}

// localSpeaker records what it was asked to play.
type recordingSpeaker struct {
	*inject.AudioOut
	mu     sync.Mutex
	chunks int
}

func localSpeaker(t *testing.T) *recordingSpeaker {
	t.Helper()
	spk := &recordingSpeaker{AudioOut: inject.NewAudioOut("speaker")}
	spk.PlayStreamFunc = func(ctx context.Context, _ *rutils.AudioInfo,
		chunks <-chan []byte, _ map[string]interface{},
	) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-chunks:
				if !ok {
					return nil
				}
				spk.mu.Lock()
				spk.chunks++
				spk.mu.Unlock()
			}
		}
	}
	return spk
}

func (s *recordingSpeaker) played() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunks
}

func radioDeps(mic audioin.AudioIn, spk audioout.AudioOut, up audioout.AudioOut, down audioin.AudioIn) resource.Dependencies {
	deps := resource.Dependencies{}
	if mic != nil {
		deps[audioin.Named("mic")] = mic
	}
	if spk != nil {
		deps[audioout.Named("speaker")] = spk
	}
	if up != nil {
		deps[audioout.Named("hub-uplink")] = up
	}
	if down != nil {
		deps[audioin.Named("hub-downlink")] = down
	}
	return deps
}

func testRadioConfig() *RadioConfig {
	return &RadioConfig{
		Source: "mic", Sink: "speaker",
		Uplink: "hub-uplink", Downlink: "hub-downlink",
		Member: "alpha", Channel: "ops",
		SampleRate: 16000, NumChannels: 1,
	}
}

func newTestRadio(t *testing.T, deps resource.Dependencies, cfg *RadioConfig) resource.Resource {
	t.Helper()
	r, err := NewRadio(context.Background(), deps, generic.Named("radio"), cfg, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRadioIsBuiltWithoutItsEndpoints: the hub is normally a remote, and a
// required dependency on a remote blocks construction whenever that part is
// asleep. A radio that cannot be built is a radio that cannot tell you why.
func TestRadioIsBuiltWithoutItsEndpoints(t *testing.T) {
	r := newTestRadio(t, resource.Dependencies{}, testRadioConfig())

	resp, err := r.DoCommand(context.Background(), map[string]interface{}{"stats": true})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, resp["ready"], test.ShouldEqual, false)
	for _, key := range []string{"source_available", "sink_available", "uplink_available", "downlink_available"} {
		test.That(t, resp[key], test.ShouldEqual, false)
	}
	// And it says which half is missing rather than merely failing.
	test.That(t, resp["can_talk"], test.ShouldEqual, false)
	test.That(t, resp["can_listen"], test.ShouldEqual, false)
}

func TestRadioCannotTalkWithoutAMicrophone(t *testing.T) {
	up, down := hubEndpoints(t)
	deps := radioDeps(nil, localSpeaker(t), up, down)
	r := newTestRadio(t, deps, testRadioConfig())

	_, err := r.DoCommand(context.Background(), map[string]interface{}{"talk": true})
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, err.Error(), test.ShouldContainSubstring, "cannot talk")
}

// TestRadioCarriesAudioToAnotherMember is the whole point of the module, end to
// end through the model layer: alpha keys up and bravo hears it.
func TestRadioCarriesAudioToAnotherMember(t *testing.T) {
	up, down := hubEndpoints(t)

	alphaSpeaker := localSpeaker(t)
	alpha := newTestRadio(t, radioDeps(localMic(t), alphaSpeaker, up, down), testRadioConfig())

	bravoCfg := testRadioConfig()
	bravoCfg.Member = "bravo"
	bravoSpeaker := localSpeaker(t)
	newTestRadio(t, radioDeps(localMic(t), bravoSpeaker, up, down), bravoCfg)

	_, err := alpha.DoCommand(context.Background(), map[string]interface{}{"talk": true})
	test.That(t, err, test.ShouldBeNil)

	eventually(t, "bravo to hear alpha", func() bool { return bravoSpeaker.played() > 3 })

	// And alpha does not hear themselves, which would be an acoustic feedback
	// loop the moment a real microphone and speaker were in the same room.
	test.That(t, alphaSpeaker.played(), test.ShouldEqual, 0)
}

// TestRetuneMovesBothPumps: the point of the model is that this needs no config
// change.
func TestRetuneMovesBothPumps(t *testing.T) {
	up, down := hubEndpoints(t, "ops", "logistics")
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())
	ctx := context.Background()

	resp, err := r.DoCommand(ctx, map[string]interface{}{ChannelKey: "logistics"})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, resp[ChannelKey], test.ShouldEqual, "logistics")

	stats, err := r.DoCommand(ctx, map[string]interface{}{"stats": true})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, stats["channel"], test.ShouldEqual, "logistics")
	test.That(t, stats["retunes"].(int64), test.ShouldEqual, 1)
}

// TestRetuneToAnUnknownChannelIsRefusedUpFront: left to the async path this is
// silent deafness, with the reason buried in a stat nobody is reading.
func TestRetuneToAnUnknownChannelIsRefusedUpFront(t *testing.T) {
	up, down := hubEndpoints(t, "ops", "logistics")
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())

	_, err := r.DoCommand(context.Background(), map[string]interface{}{ChannelKey: "nope"})
	test.That(t, err, test.ShouldNotBeNil)
	// The error lists what is on offer, so the fix is obvious.
	test.That(t, err.Error(), test.ShouldContainSubstring, "logistics")

	stats, _ := r.DoCommand(context.Background(), map[string]interface{}{"stats": true})
	test.That(t, stats["channel"], test.ShouldEqual, "ops")
}

// TestRetuneIsAllowedWhenTheHubIsUnreachable: refusing here would make a
// disconnected radio impossible to pre-tune, and it is already reporting that
// it cannot reach the hub.
func TestRetuneIsAllowedWhenTheHubIsUnreachable(t *testing.T) {
	r := newTestRadio(t, resource.Dependencies{}, testRadioConfig())

	_, err := r.DoCommand(context.Background(), map[string]interface{}{ChannelKey: "anything"})
	test.That(t, err, test.ShouldBeNil)
}

func TestRadioCommandOrdering(t *testing.T) {
	up, down := hubEndpoints(t, "ops", "logistics")
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())

	// Retune and key up in one call. The radio applies channel first, so this
	// keys up on logistics, never on ops.
	resp, err := r.DoCommand(context.Background(), map[string]interface{}{
		ChannelKey: "logistics", "talk": true,
	})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, resp[ChannelKey], test.ShouldEqual, "logistics")
	test.That(t, resp["talking"], test.ShouldEqual, true)
}

func TestRadioRejectsBadCommands(t *testing.T) {
	up, down := hubEndpoints(t)
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		cmd  map[string]interface{}
	}{
		{"empty", map[string]interface{}{}},
		{"unrecognised", map[string]interface{}{"nonsense": 1}},
		{"non-bool talk", map[string]interface{}{"talk": "yes"}},
		{"non-string channel", map[string]interface{}{ChannelKey: 42}},
		{"unknown gate mode", map[string]interface{}{"gate_mode": "sometimes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := r.DoCommand(ctx, tc.cmd)
			test.That(t, err, test.ShouldNotBeNil)
		})
	}
}

// TestRadioAcceptsSecondsAsFloat64: structpb turns every JSON number into a
// float64, so this is the shape that actually arrives over the wire.
func TestRadioAcceptsSecondsAsFloat64(t *testing.T) {
	up, down := hubEndpoints(t)
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())

	resp, err := r.DoCommand(context.Background(), map[string]interface{}{
		"talk": true, "seconds": float64(2),
	})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, resp["talking"], test.ShouldEqual, true)
}

func TestRadioStatusIsJSONSafe(t *testing.T) {
	up, down := hubEndpoints(t)
	r := newTestRadio(t, radioDeps(localMic(t), localSpeaker(t), up, down), testRadioConfig())

	st, err := r.Status(context.Background())
	test.That(t, err, test.ShouldBeNil)
	assertJSONSafe(t, "radio status", st)

	// Both directions must be represented, since the two pumps are the whole
	// thing an operator needs to see.
	for _, key := range []string{"tx_chunks_in", "rx_chunks_in", "hub_heartbeat_age_ms", "busy_rejections"} {
		_, ok := st[key]
		test.That(t, ok, test.ShouldBeTrue)
	}
}

func TestClosedRadioRefusesFurtherUse(t *testing.T) {
	up, down := hubEndpoints(t)
	r, err := NewRadio(context.Background(),
		radioDeps(localMic(t), localSpeaker(t), up, down),
		generic.Named("radio"), testRadioConfig(), logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, r.Close(context.Background()), test.ShouldBeNil)

	_, closedErr := r.DoCommand(context.Background(), map[string]interface{}{"talk": true})
	test.That(t, closedErr, test.ShouldWrap, errRadioClosed)
}

// TestClassifyHubError checks the mapping a radio hands the pump. Getting this
// wrong is what makes a held talk button open fifty streams a second.
func TestClassifyHubError(t *testing.T) {
	busy, _ := classifyHubError(bus.ToStatus(bus.BusyError("bravo")))
	test.That(t, busy, test.ShouldBeGreaterThan, time.Duration(0))

	revoked, _ := classifyHubError(status.Error(codes.Aborted, bus.RevokedMsg))
	test.That(t, revoked, test.ShouldBeGreaterThan, time.Duration(0))
	// A revocation is expected to clear sooner than a channel somebody holds.
	test.That(t, revoked, test.ShouldBeLessThan, busy)

	// Hardest of all: retrying cannot conjure a channel the hub does not carry.
	missing, _ := classifyHubError(status.Error(codes.NotFound, "no such channel"))
	test.That(t, missing, test.ShouldBeGreaterThan, busy)

	// Anything else is somebody else's problem and must be reported verbatim.
	unclassified, _ := classifyHubError(errors.New("speaker on fire"))
	test.That(t, unclassified, test.ShouldEqual, time.Duration(0))
}
