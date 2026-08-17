package walkie

import (
	"context"
	"errors"
	"testing"

	"go.viam.com/rdk/components/generic"
	toggleswitch "go.viam.com/rdk/components/switch"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/test"
)

// fakeRadio records what a switch asked of it and answers with a state the
// switch has to read back correctly.
type fakeRadio struct {
	resource.Named
	resource.TriviallyReconfigurable
	resource.TriviallyCloseable

	lastCmd map[string]interface{}

	// state is what {"stats": true} reports.
	channel  string
	talking  bool
	gateMode string

	err error
}

func (f *fakeRadio) Name() resource.Name { return generic.Named("radio") }

func (f *fakeRadio) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if f.err != nil {
		return nil, f.err
	}
	if v, ok := cmd["stats"]; ok && truthy(v) {
		return map[string]interface{}{
			"channel":   f.channel,
			"talking":   f.talking,
			"gate_mode": f.gateMode,
		}, nil
	}

	f.lastCmd = cmd
	if v, ok := cmd[ChannelKey].(string); ok {
		f.channel = v
	}
	if v, ok := cmd["talk"].(bool); ok {
		f.talking = v
	}
	if v, ok := cmd["gate_mode"].(string); ok {
		f.gateMode = v
	}
	return map[string]interface{}{}, nil
}

func (f *fakeRadio) Status(context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func newTestSwitch(t *testing.T, r *fakeRadio, cfg *SwitchConfig) toggleswitch.Switch {
	t.Helper()
	deps := resource.Dependencies{generic.Named("radio"): r}
	conf := resource.Config{
		Name: "sw", API: toggleswitch.API, Model: Switch, ConvertedAttributes: cfg,
	}
	s, err := newWalkieSwitch(context.Background(), deps, conf, logging.NewTestLogger(t))
	test.That(t, err, test.ShouldBeNil)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestPTTSwitchDrivesTheGate(t *testing.T) {
	r := &fakeRadio{gateMode: "manual"}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio", Positions: []string{"idle", "talk", "vox"}})
	ctx := context.Background()

	test.That(t, s.SetPosition(ctx, 1, nil), test.ShouldBeNil)
	test.That(t, r.talking, test.ShouldBeTrue)
	// Both keys, so arriving from vox closes the vox gate as well as opening
	// the manual one.
	test.That(t, r.lastCmd["gate_mode"], test.ShouldEqual, "manual")

	test.That(t, s.SetPosition(ctx, 2, nil), test.ShouldBeNil)
	test.That(t, r.gateMode, test.ShouldEqual, "vox")

	test.That(t, s.SetPosition(ctx, 0, nil), test.ShouldBeNil)
	test.That(t, r.talking, test.ShouldBeFalse)
}

func TestChannelSwitchRetunes(t *testing.T) {
	r := &fakeRadio{channel: "ops"}
	s := newTestSwitch(t, r, &SwitchConfig{
		Radio: "radio", Mode: "channel", Positions: []string{"ops", "logistics"},
	})
	ctx := context.Background()

	test.That(t, s.SetPosition(ctx, 1, nil), test.ShouldBeNil)
	test.That(t, r.channel, test.ShouldEqual, "logistics")

	pos, err := s.GetPosition(ctx, nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, pos, test.ShouldEqual, 1)
}

// TestGetPositionReadsTheRadio: a switch must tell the truth when something
// else moved the radio underneath it, not report back whatever it last set.
func TestGetPositionReadsTheRadio(t *testing.T) {
	r := &fakeRadio{channel: "ops", gateMode: "manual"}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio", Positions: []string{"idle", "talk"}})
	ctx := context.Background()

	// Somebody else keys up through DoCommand.
	r.talking = true

	pos, err := s.GetPosition(ctx, nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, pos, test.ShouldEqual, 1)
}

// TestGetPositionFallsBackWhenTheRadioIsUnreachable: better to report the last
// position we set than to fail a read the app makes constantly.
func TestGetPositionFallsBackWhenTheRadioIsUnreachable(t *testing.T) {
	r := &fakeRadio{gateMode: "manual"}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio", Positions: []string{"idle", "talk"}})
	ctx := context.Background()

	test.That(t, s.SetPosition(ctx, 1, nil), test.ShouldBeNil)
	r.err = errors.New("hub unreachable")

	pos, err := s.GetPosition(ctx, nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, pos, test.ShouldEqual, 1)
}

// TestGetPositionFallsBackOnAnUnrepresentableState: a two-position toggle has
// no position for vox, and a channel switch has none for a channel it does not
// offer. Reporting position zero would be a worse lie.
func TestGetPositionFallsBackOnAnUnrepresentableState(t *testing.T) {
	r := &fakeRadio{gateMode: "manual"}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio", Positions: []string{"idle", "talk"}})
	ctx := context.Background()

	test.That(t, s.SetPosition(ctx, 1, nil), test.ShouldBeNil)
	r.talking = false
	r.gateMode = "vox"

	pos, err := s.GetPosition(ctx, nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, pos, test.ShouldEqual, 1)
}

func TestSwitchRejectsOutOfRangePosition(t *testing.T) {
	r := &fakeRadio{}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio"})

	test.That(t, s.SetPosition(context.Background(), 7, nil), test.ShouldNotBeNil)
}

func TestSwitchReportsItsPositions(t *testing.T) {
	r := &fakeRadio{}
	s := newTestSwitch(t, r, &SwitchConfig{
		Radio: "radio", Mode: "channel", Positions: []string{"ops", "logistics", "yard"},
	})

	count, labels, err := s.GetNumberOfPositions(context.Background(), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, count, test.ShouldEqual, 3)
	// The labels are the channel names, which is what makes this readable on
	// the machine page.
	test.That(t, labels, test.ShouldResemble, []string{"ops", "logistics", "yard"})
}

// TestSwitchPassesDoCommandThrough means a switch is a complete control surface
// on its own, without also having to reach the radio.
func TestSwitchPassesDoCommandThrough(t *testing.T) {
	r := &fakeRadio{}
	s := newTestSwitch(t, r, &SwitchConfig{Radio: "radio"})

	_, err := s.DoCommand(context.Background(), map[string]interface{}{ChannelKey: "yard"})
	test.That(t, err, test.ShouldBeNil)
	test.That(t, r.channel, test.ShouldEqual, "yard")
}
