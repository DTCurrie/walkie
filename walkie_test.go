package walkie

import (
	"context"
	"testing"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/bus"
)

// --- config validation -------------------------------------------------------

func TestBusConfigValidate(t *testing.T) {
	ok := func(t *testing.T, cfg *BusConfig) {
		t.Helper()
		_, _, err := cfg.Validate("p")
		test.That(t, err, test.ShouldBeNil)
	}
	bad := func(t *testing.T, cfg *BusConfig, wantSubstring string) {
		t.Helper()
		_, _, err := cfg.Validate("p")
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, wantSubstring)
	}

	ok(t, &BusConfig{Channels: []ChannelConfig{{Name: "ops"}}})
	bad(t, &BusConfig{}, "channels")
	bad(t, &BusConfig{Channels: []ChannelConfig{{}}}, "name")
	bad(t, &BusConfig{Channels: []ChannelConfig{{Name: "ops"}, {Name: "ops"}}}, "more than once")
	bad(t, &BusConfig{
		Channels:   []ChannelConfig{{Name: "ops"}},
		SampleRate: 16000,
	}, "both sample_rate and num_channels")
	bad(t, &BusConfig{
		Channels:        []ChannelConfig{{Name: "ops"}},
		FloorHangoverMs: -1,
	}, "must not be negative")

	// A hangover shorter than a talker's own stream teardown would let a
	// bystander take the channel during a breath.
	bad(t, &BusConfig{
		Channels:        []ChannelConfig{{Name: "ops"}},
		FloorHangoverMs: 50,
	}, "shorter than")
}

func TestBusHasNoDependencies(t *testing.T) {
	cfg := &BusConfig{Channels: []ChannelConfig{{Name: "ops"}}}
	required, optional, err := cfg.Validate("p")
	test.That(t, err, test.ShouldBeNil)
	// The endpoints depend on the bus, never the other way round.
	test.That(t, required, test.ShouldBeEmpty)
	test.That(t, optional, test.ShouldBeEmpty)
}

func TestEndpointConfigsRequireTheirBus(t *testing.T) {
	up := &UplinkConfig{Bus: "bus"}
	required, optional, err := up.Validate("p")
	test.That(t, err, test.ShouldBeNil)
	// Required, not optional: the bus is an in-process object on this very
	// machine, so there is no sleeping-remote problem to tolerate.
	test.That(t, required, test.ShouldResemble, []string{"bus"})
	test.That(t, optional, test.ShouldBeEmpty)

	_, _, downErr := (&DownlinkConfig{Bus: "bus"}).Validate("p")
	test.That(t, downErr, test.ShouldBeNil)
	_, _, missingErr := (&UplinkConfig{}).Validate("p")
	test.That(t, missingErr, test.ShouldNotBeNil)
}

func TestRadioConfigValidate(t *testing.T) {
	base := func() *RadioConfig {
		return &RadioConfig{
			Source: "mic", Sink: "speaker",
			Uplink: "hub-uplink", Downlink: "hub-downlink",
			Channel: "ops",
		}
	}

	required, optional, err := base().Validate("p")
	test.That(t, err, test.ShouldBeNil)
	// All four endpoints optional: two normally live on the hub, and a required
	// dependency on a remote blocks construction whenever that part sleeps.
	test.That(t, len(required), test.ShouldEqual, 0)
	test.That(t, len(optional), test.ShouldEqual, 4)

	for _, field := range []string{"source", "sink", "uplink", "downlink", "channel"} {
		cfg := base()
		switch field {
		case "source":
			cfg.Source = ""
		case "sink":
			cfg.Sink = ""
		case "uplink":
			cfg.Uplink = ""
		case "downlink":
			cfg.Downlink = ""
		case "channel":
			cfg.Channel = ""
		}
		_, _, err := cfg.Validate("p")
		test.That(t, err, test.ShouldNotBeNil)
	}

	cfg := base()
	cfg.GateMode = "sometimes"
	_, _, gateErr := cfg.Validate("p")
	test.That(t, gateErr, test.ShouldNotBeNil)
}

// TestRejectsRemoteNamesWithColons covers the single most common way to
// misconfigure a module like this. Lookup skips its short-name scan whenever a
// name carries a remote segment, so "hub:uplink" resolves to nothing and fails
// with a bare "dependency not found".
func TestRejectsRemoteNamesWithColons(t *testing.T) {
	cfg := &RadioConfig{
		Source: "mic", Sink: "speaker",
		Uplink: "hub:uplink", Downlink: "hub-downlink", Channel: "ops",
	}
	_, _, err := cfg.Validate("p")
	test.That(t, err, test.ShouldNotBeNil)
	// The error has to name the fix, not just the problem.
	test.That(t, err.Error(), test.ShouldContainSubstring, "prefix")

	fq := &RadioConfig{
		Source: "rdk:component:audio_in/mic", Sink: "speaker",
		Uplink: "hub-uplink", Downlink: "hub-downlink", Channel: "ops",
	}
	_, _, fqErr := fq.Validate("p")
	test.That(t, fqErr, test.ShouldNotBeNil)
}

func TestSwitchConfigValidate(t *testing.T) {
	_, _, err := (&SwitchConfig{}).Validate("p")
	test.That(t, err, test.ShouldNotBeNil)

	required, _, err := (&SwitchConfig{Radio: "radio"}).Validate("p")
	test.That(t, err, test.ShouldBeNil)
	test.That(t, required, test.ShouldResemble, []string{"radio"})

	// A channel switch cannot guess which channels this machine should offer.
	_, _, noPositionsErr := (&SwitchConfig{Radio: "r", Mode: "channel"}).Validate("p")
	test.That(t, noPositionsErr, test.ShouldNotBeNil)
	_, _, positionsErr := (&SwitchConfig{
		Radio: "r", Mode: "channel", Positions: []string{"ops", "logistics"},
	}).Validate("p")
	test.That(t, positionsErr, test.ShouldBeNil)
	_, _, dupErr := (&SwitchConfig{
		Radio: "r", Positions: []string{"idle", "idle"},
	}).Validate("p")
	test.That(t, dupErr, test.ShouldNotBeNil)
	_, _, badModeErr := (&SwitchConfig{Radio: "r", Mode: "nonsense"}).Validate("p")
	test.That(t, badModeErr, test.ShouldNotBeNil)
}

// --- identity ----------------------------------------------------------------

func TestResolveIdentity(t *testing.T) {
	t.Run("extra wins over config defaults", func(t *testing.T) {
		id, err := resolveIdentity(
			map[string]interface{}{ChannelKey: "ops", MemberKey: "alpha"},
			"default-channel", "default-member")
		test.That(t, err, test.ShouldBeNil)
		test.That(t, id, test.ShouldResemble, identity{Channel: "ops", Member: "alpha"})
	})

	t.Run("config defaults fill the gaps", func(t *testing.T) {
		id, err := resolveIdentity(nil, "ops", "alpha")
		test.That(t, err, test.ShouldBeNil)
		test.That(t, id, test.ShouldResemble, identity{Channel: "ops", Member: "alpha"})
	})

	t.Run("a missing member is refused, not defaulted", func(t *testing.T) {
		// It cannot be defaulted: self-echo suppression matches on this name, so
		// a blank one would match every other blank one and mute them all.
		_, err := resolveIdentity(map[string]interface{}{ChannelKey: "ops"}, "", "")
		test.That(t, err, test.ShouldNotBeNil)
	})

	t.Run("a missing channel is refused", func(t *testing.T) {
		_, err := resolveIdentity(nil, "", "alpha")
		test.That(t, err, test.ShouldNotBeNil)
	})

	t.Run("wrong types are rejected", func(t *testing.T) {
		_, err := resolveIdentity(map[string]interface{}{ChannelKey: 42}, "", "alpha")
		test.That(t, err, test.ShouldNotBeNil)
	})
}

// --- the hub endpoints -------------------------------------------------------

func newTestBusResource(t *testing.T, channels ...string) resource.Resource {
	t.Helper()
	if len(channels) == 0 {
		channels = []string{"ops"}
	}
	chans := make([]ChannelConfig, 0, len(channels))
	for _, c := range channels {
		chans = append(chans, ChannelConfig{Name: c})
	}

	res, err := NewBus(context.Background(), generic.Named("bus"),
		&BusConfig{Channels: chans}, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = res.Close(context.Background()) })
	return res
}

func depsWithBus(res resource.Resource) resource.Dependencies {
	return resource.Dependencies{generic.Named("bus"): res}
}

func newTestUplink(t *testing.T, deps resource.Dependencies, cfg *UplinkConfig) audioout.AudioOut {
	t.Helper()
	conf := resource.Config{
		Name: "uplink", API: audioout.API, Model: Uplink, ConvertedAttributes: cfg,
	}
	u, err := newUplink(context.Background(), deps, conf, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = u.Close(context.Background()) })
	return u
}

func newTestDownlink(t *testing.T, deps resource.Dependencies, cfg *DownlinkConfig) audioin.AudioIn {
	t.Helper()
	conf := resource.Config{
		Name: "downlink", API: audioin.API, Model: Downlink, ConvertedAttributes: cfg,
	}
	d, err := newDownlink(context.Background(), deps, conf, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = d.Close(context.Background()) })
	return d
}

// TestEndpointRejectsNonLocalBus covers the one wiring mistake the config alone
// cannot catch. A module dependency that is local arrives as the concrete Go
// object; anything else arrives as an RPC client, which shares no routing state
// with the bus at all.
func TestEndpointRejectsNonLocalBus(t *testing.T) {
	// A stand-in for what viam-server hands back for a resource on another part.
	notABus := &fakeGeneric{}
	deps := resource.Dependencies{generic.Named("bus"): notABus}

	conf := resource.Config{
		Name: "uplink", API: audioout.API, Model: Uplink,
		ConvertedAttributes: &UplinkConfig{Bus: "bus"},
	}
	_, err := newUplink(context.Background(), deps, conf, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, err, test.ShouldWrap, errNotLocalBus)
	// The message has to explain the constraint, since nothing else will.
	test.That(t, err.Error(), test.ShouldContainSubstring, "same machine")
}

// TestUplinkPlayStreamCarriesAudio is the end-to-end path through the model
// layer: a talker on the uplink reaches a listener on the downlink.
func TestUplinkPlayStreamCarriesAudio(t *testing.T) {
	busRes := newTestBusResource(t)
	deps := depsWithBus(busRes)
	up := newTestUplink(t, deps, &UplinkConfig{Bus: "bus"})
	down := newTestDownlink(t, deps, &DownlinkConfig{Bus: "bus"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listening, err := down.GetAudio(ctx, rutils.CodecPCM16, 0, 0,
		map[string]interface{}{ChannelKey: "ops", MemberKey: "bravo"})
	test.That(t, err, test.ShouldBeNil)

	heard := make(chan int, 1)
	go func() {
		n := 0
		for chunk := range listening {
			if len(chunk.AudioData) > 0 {
				n++
				if n == 3 {
					heard <- n
				}
			}
		}
	}()

	chunks := make(chan []byte)
	done := make(chan error, 1)
	go func() {
		done <- up.PlayStream(ctx, testAudioInfo(), chunks,
			map[string]interface{}{ChannelKey: "ops", MemberKey: "alpha"})
	}()
	for range 3 {
		chunks <- make([]byte, 320)
	}
	close(chunks)

	test.That(t, <-done, test.ShouldBeNil)
	select {
	case <-heard:
	case <-ctx.Done():
		t.Fatal("the listener never heard the transmission")
	}
}

// TestUplinkReturnsNonNilWhenItDoesNotDrain is the wedge invariant. The RDK's
// audioout server blocks on its receive goroutine after a nil return, and that
// goroutine is itself blocked writing into a channel nobody reads any more, so
// a nil return without a full drain hangs the talker.
func TestUplinkReturnsNonNilWhenItDoesNotDrain(t *testing.T) {
	deps := depsWithBus(newTestBusResource(t))
	up := newTestUplink(t, deps, &UplinkConfig{Bus: "bus"})
	ctx := context.Background()

	cases := []struct {
		name  string
		info  *rutils.AudioInfo
		extra map[string]interface{}
	}{
		{
			name:  "unknown channel",
			info:  testAudioInfo(),
			extra: map[string]interface{}{ChannelKey: "nope", MemberKey: "alpha"},
		},
		{
			name:  "no identity",
			info:  testAudioInfo(),
			extra: nil,
		},
		{
			name:  "no format",
			info:  nil,
			extra: map[string]interface{}{ChannelKey: "ops", MemberKey: "alpha"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A channel nobody ever writes to or closes: if the implementation
			// tried to drain, this would hang rather than return.
			chunks := make(chan []byte)
			err := up.PlayStream(ctx, tc.info, chunks, tc.extra)
			test.That(t, err, test.ShouldNotBeNil)
		})
	}
}

// TestSecondTalkerIsRefusedThroughTheEndpoint checks the busy path survives the
// trip through the model layer with a code a radio can still recognise.
func TestSecondTalkerIsRefusedThroughTheEndpoint(t *testing.T) {
	deps := depsWithBus(newTestBusResource(t))
	up := newTestUplink(t, deps, &UplinkConfig{Bus: "bus"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Alpha takes the floor and holds it.
	first := make(chan []byte)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- up.PlayStream(ctx, testAudioInfo(), first,
			map[string]interface{}{ChannelKey: "ops", MemberKey: "alpha"})
	}()
	first <- make([]byte, 320)

	// Bravo is refused, and told why in a way classifyHubError understands.
	err := up.PlayStream(ctx, testAudioInfo(), make(chan []byte),
		map[string]interface{}{ChannelKey: "ops", MemberKey: "bravo"})
	test.That(t, bus.IsBusy(err), test.ShouldBeTrue)
	backoff, _ := classifyHubError(err)
	test.That(t, backoff, test.ShouldBeGreaterThan, time.Duration(0))

	close(first)
	<-firstDone
}

func TestUplinkRefusesPlay(t *testing.T) {
	deps := depsWithBus(newTestBusResource(t))
	up := newTestUplink(t, deps, &UplinkConfig{Bus: "bus"})

	err := up.Play(context.Background(), make([]byte, 320), testAudioInfo(), nil)
	test.That(t, status.Code(err), test.ShouldEqual, codes.Unimplemented)
	// And it must say what to use instead.
	test.That(t, err.Error(), test.ShouldContainSubstring, "PlayStream")
}

func TestEndpointsListChannels(t *testing.T) {
	deps := depsWithBus(newTestBusResource(t, "ops", "logistics"))
	up := newTestUplink(t, deps, &UplinkConfig{Bus: "bus"})
	down := newTestDownlink(t, deps, &DownlinkConfig{Bus: "bus"})

	// A radio asks this before retuning, so a bad channel is refused up front
	// rather than becoming silent deafness.
	for name, res := range map[string]interface {
		DoCommand(context.Context, map[string]interface{}) (map[string]interface{}, error)
	}{
		"uplink":   up,
		"downlink": down,
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := res.DoCommand(context.Background(), map[string]interface{}{"channels": true})
			test.That(t, err, test.ShouldBeNil)
			got, ok := resp["channels"].([]interface{})
			test.That(t, ok, test.ShouldBeTrue)
			test.That(t, got, test.ShouldHaveLength, 2)
		})
	}
}

func TestDownlinkRejectsUnknownChannel(t *testing.T) {
	deps := depsWithBus(newTestBusResource(t))
	down := newTestDownlink(t, deps, &DownlinkConfig{Bus: "bus"})

	_, err := down.GetAudio(context.Background(), rutils.CodecPCM16, 0, 0,
		map[string]interface{}{ChannelKey: "nope", MemberKey: "alpha"})
	test.That(t, status.Code(err), test.ShouldEqual, codes.NotFound)
}

// --- the bus model -----------------------------------------------------------

func TestBusDoCommand(t *testing.T) {
	res := newTestBusResource(t, "ops", "logistics")
	ctx := context.Background()

	_, err := res.DoCommand(ctx, map[string]interface{}{})
	test.That(t, err, test.ShouldNotBeNil)
	_, unknownCmdErr := res.DoCommand(ctx, map[string]interface{}{"nonsense": true})
	test.That(t, unknownCmdErr, test.ShouldNotBeNil)

	resp, err := res.DoCommand(ctx, map[string]interface{}{"stats": true})
	test.That(t, err, test.ShouldBeNil)
	detail, ok := resp["channels_detail"].([]interface{})
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, detail, test.ShouldHaveLength, 2)

	_, badChannelErr := res.DoCommand(ctx, map[string]interface{}{"clear_floor": "nope"})
	test.That(t, badChannelErr, test.ShouldNotBeNil)
}

// TestBusStatusIsJSONSafe: everything here crosses a structpb round trip, which
// only carries a handful of types.
func TestBusStatusIsJSONSafe(t *testing.T) {
	res := newTestBusResource(t, "ops")
	st, err := res.Status(context.Background())
	test.That(t, err, test.ShouldBeNil)
	assertJSONSafe(t, "bus status", st)
}

func TestBusRefusesUseAfterClose(t *testing.T) {
	res, err := NewBus(context.Background(), generic.Named("bus"),
		&BusConfig{Channels: []ChannelConfig{{Name: "ops"}}}, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, res.Close(context.Background()), test.ShouldBeNil)
	_, closedErr := res.DoCommand(context.Background(), map[string]interface{}{"stats": true})
	test.That(t, closedErr, test.ShouldNotBeNil)
}

// --- helpers -----------------------------------------------------------------

func testAudioInfo() *rutils.AudioInfo {
	return &rutils.AudioInfo{Codec: rutils.CodecPCM16, SampleRateHz: 16000, NumChannels: 1}
}

// assertJSONSafe walks a map and fails on any value structpb cannot carry.
func assertJSONSafe(t *testing.T, what string, m map[string]interface{}) {
	t.Helper()
	for k, v := range m {
		switch value := v.(type) {
		case nil, bool, string, float64, int64:
		case []interface{}:
			for i, item := range value {
				switch item.(type) {
				case nil, bool, string, float64, int64, map[string]interface{}:
				default:
					t.Errorf("%s[%q][%d] is %T, which does not survive structpb", what, k, i, item)
				}
			}
		case map[string]interface{}:
			assertJSONSafe(t, what+"."+k, value)
		default:
			t.Errorf("%s[%q] is %T, which does not survive structpb", what, k, v)
		}
	}
}

// fakeGeneric stands in for a resource that answers DoCommand but is not a bus,
// which is what an RPC client for a bus on another machine looks like.
type fakeGeneric struct {
	resource.Named
	resource.TriviallyReconfigurable
	resource.TriviallyCloseable

	lastCmd map[string]interface{}
	resp    map[string]interface{}
	err     error
}

func (f *fakeGeneric) Name() resource.Name { return generic.Named("bus") }

func (f *fakeGeneric) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	f.lastCmd = cmd
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeGeneric) Status(context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}
