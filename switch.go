package walkie

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.viam.com/rdk/components/generic"
	toggleswitch "go.viam.com/rdk/components/switch"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

// Switch is the model triple for this module's radio switch.
var Switch = resource.NewModel("dtcurrie", "walkie", "switch")

func init() {
	resource.RegisterComponent(toggleswitch.API, Switch,
		resource.Registration[toggleswitch.Switch, *SwitchConfig]{
			Constructor: newWalkieSwitch,
		},
	)
}

// SwitchMode is what a switch's positions mean.
type SwitchMode string

const (
	// ModePTT binds positions to gate states, so the switch is a talk button.
	ModePTT SwitchMode = "ptt"
	// ModeChannel binds positions to channel names, so the switch is a dial.
	// This is what makes changing channel a first-class control on the machine
	// page rather than a DoCommand somebody has to remember.
	ModeChannel SwitchMode = "channel"
)

// Gate is one gate state a ptt switch position can select.
type Gate string

// The gate states a ptt position may be bound to.
const (
	// GateIdle closes the gate: this radio transmits nothing.
	GateIdle Gate = "idle"
	// GateTalk holds the gate open for as long as the switch stays here.
	GateTalk Gate = "talk"
	// GateVox opens the gate on sound and closes it after the hangover.
	GateVox Gate = "vox"
)

// defaultPTTPositions is a plain two-position toggle: off, and talking.
var defaultPTTPositions = []string{string(GateIdle), string(GateTalk)}

func parseGate(s string) (Gate, error) {
	switch g := Gate(strings.ToLower(strings.TrimSpace(s))); g {
	case GateIdle, GateTalk, GateVox:
		return g, nil
	default:
		return "", fmt.Errorf("unknown position %q, expected one of %q, %q, %q",
			s, GateIdle, GateTalk, GateVox)
	}
}

// SwitchConfig is the JSON config for the switch model.
type SwitchConfig struct {
	// Radio is the name of the dtcurrie:walkie:radio this switch drives.
	Radio string `json:"radio"`

	// Mode is "ptt" (the default) or "channel".
	Mode string `json:"mode,omitempty"`

	// Positions names what each switch position selects, in order. In ptt mode
	// these are gate states, defaulting to ["idle", "talk"]; in channel mode they
	// are channel names and are required.
	Positions []string `json:"positions,omitempty"`
}

// Validate checks the config and reports the radio as a required dependency. A
// switch with nothing to drive has no reason to exist, and the two always live
// on the same machine.
func (cfg *SwitchConfig) Validate(path string) ([]string, []string, error) {
	if cfg.Radio == "" {
		return nil, nil, resource.NewConfigValidationFieldRequiredError(path, "radio")
	}
	if err := validateResourceRef(path, "radio", cfg.Radio); err != nil {
		return nil, nil, err
	}
	if _, err := parseSwitchMode(cfg.Mode); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	if _, err := resolvePositions(cfg.Mode, cfg.Positions); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	return []string{cfg.Radio}, nil, nil
}

func parseSwitchMode(s string) (SwitchMode, error) {
	switch m := SwitchMode(strings.ToLower(strings.TrimSpace(s))); m {
	case "", ModePTT:
		return ModePTT, nil
	case ModeChannel:
		return ModeChannel, nil
	default:
		return "", fmt.Errorf(`mode must be %q or %q, got %q`, ModePTT, ModeChannel, s)
	}
}

// resolvePositions validates the positions for a mode and returns them.
func resolvePositions(rawMode string, raw []string) ([]string, error) {
	mode, err := parseSwitchMode(rawMode)
	if err != nil {
		return nil, err
	}

	if len(raw) == 0 {
		if mode == ModeChannel {
			return nil, errors.New(`a channel switch needs "positions": the channel names to ` +
				`offer, e.g. ["ops", "logistics"]`)
		}
		return defaultPTTPositions, nil
	}
	if len(raw) < 2 {
		return nil, errors.New("positions needs at least 2 entries")
	}

	out := make([]string, 0, len(raw))
	seen := map[string]bool{}
	for _, s := range raw {
		value := s
		if mode == ModePTT {
			g, err := parseGate(s)
			if err != nil {
				return nil, err
			}
			value = string(g)
		} else if strings.TrimSpace(s) == "" {
			return nil, errors.New("a channel name must not be blank")
		}
		if seen[value] {
			return nil, fmt.Errorf("position %q is listed more than once", value)
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

// radioController is the part of a radio a switch drives. The radio satisfies it
// directly when both are built by the same module, and the generic client when
// they are not.
type radioController interface {
	DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error)
}

type walkieSwitch struct {
	resource.AlwaysRebuild
	resource.Named

	logger    logging.Logger
	radio     radioController
	radioName string
	mode      SwitchMode
	positions []string

	// last is the position we most recently selected, used as the answer when
	// the radio cannot be read.
	mu   sync.Mutex
	last uint32
}

var _ toggleswitch.Switch = (*walkieSwitch)(nil)

func newWalkieSwitch(
	_ context.Context,
	deps resource.Dependencies,
	rawConf resource.Config,
	logger logging.Logger,
) (toggleswitch.Switch, error) {
	conf, err := resource.NativeConfig[*SwitchConfig](rawConf)
	if err != nil {
		return nil, err
	}
	mode, err := parseSwitchMode(conf.Mode)
	if err != nil {
		return nil, err
	}
	positions, err := resolvePositions(conf.Mode, conf.Positions)
	if err != nil {
		return nil, err
	}

	res, err := deps.Lookup(generic.Named(conf.Radio))
	if err != nil {
		return nil, err
	}
	ctrl, ok := res.(radioController)
	if !ok {
		return nil, fmt.Errorf("%q does not answer DoCommand, so it cannot be a walkie radio", conf.Radio)
	}

	return &walkieSwitch{
		Named:     rawConf.ResourceName().AsNamed(),
		logger:    logger,
		radio:     ctrl,
		radioName: conf.Radio,
		mode:      mode,
		positions: positions,
	}, nil
}

// commandFor maps a position onto the radio command that selects it.
func (s *walkieSwitch) commandFor(position string) map[string]interface{} {
	if s.mode == ModeChannel {
		return map[string]interface{}{ChannelKey: position}
	}
	switch Gate(position) {
	case GateVox:
		return map[string]interface{}{"gate_mode": string(GateVox)}
	case GateTalk:
		// Both keys, because arriving here from vox has to close the vox gate
		// as well as open the manual one. The radio applies them in a fixed
		// order within one call, so this is well defined.
		return map[string]interface{}{"talk": true, "gate_mode": "manual"}
	default:
		return map[string]interface{}{"talk": false, "gate_mode": "manual"}
	}
}

func (s *walkieSwitch) SetPosition(ctx context.Context, position uint32, _ map[string]interface{}) error {
	if int(position) >= len(s.positions) {
		return fmt.Errorf("position %d is out of range; this switch has %d positions (%v)",
			position, len(s.positions), s.positions)
	}
	if _, err := s.radio.DoCommand(ctx, s.commandFor(s.positions[position])); err != nil {
		return fmt.Errorf("could not drive radio %q: %w", s.radioName, err)
	}

	s.mu.Lock()
	s.last = position
	s.mu.Unlock()
	return nil
}

// GetPosition reads the radio's real state rather than whatever we last set, so
// a switch tells the truth when something else -- another switch, a DoCommand,
// vox -- moved it.
func (s *walkieSwitch) GetPosition(ctx context.Context, _ map[string]interface{}) (uint32, error) {
	resp, err := s.radio.DoCommand(ctx, map[string]interface{}{"stats": true})
	if err != nil {
		s.logger.Debugw("could not read the radio; reporting the last position we set",
			"radio", s.radioName, "error", err)
		return s.lastPosition(), nil
	}

	want, ok := s.currentValue(resp)
	if !ok {
		return s.lastPosition(), nil
	}
	for i, p := range s.positions {
		if p == want {
			return uint32(i), nil
		}
	}

	// The radio is in a state this switch has no position for -- a channel it
	// does not offer, or vox on a two-position toggle. Reporting the last
	// position is a better lie than reporting position zero.
	s.logger.Debugw("the radio is in a state this switch cannot represent",
		"radio", s.radioName, "state", want, "positions", s.positions)
	return s.lastPosition(), nil
}

// currentValue extracts whichever field of the radio's stats this switch's
// positions are drawn from.
func (s *walkieSwitch) currentValue(resp map[string]interface{}) (string, bool) {
	if s.mode == ModeChannel {
		v, ok := resp[ChannelKey].(string)
		return v, ok
	}

	// In ptt mode the answer is a combination: talking beats the gate mode,
	// because a manual gate that is open is "talk" whatever else is true.
	if talking, ok := resp["talking"].(bool); ok && talking {
		return string(GateTalk), true
	}
	mode, ok := resp["gate_mode"].(string)
	if !ok {
		return "", false
	}
	if mode == string(GateVox) {
		return string(GateVox), true
	}
	return string(GateIdle), true
}

func (s *walkieSwitch) lastPosition() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *walkieSwitch) GetNumberOfPositions(context.Context, map[string]interface{}) (uint32, []string, error) {
	labels := make([]string, len(s.positions))
	copy(labels, s.positions)
	return uint32(len(s.positions)), labels, nil
}

// DoCommand passes straight through to the radio, so a switch is a complete
// control surface on its own.
func (s *walkieSwitch) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	return s.radio.DoCommand(ctx, cmd)
}

func (s *walkieSwitch) Close(context.Context) error { return nil }
