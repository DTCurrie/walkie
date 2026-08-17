package walkie

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	goutils "go.viam.com/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DTCurrie/viam-comms/audio/pcm"
	"walkie/internal/bus"
	"walkie/internal/pump"
)

// Radio is the model triple for a member's radio.
var Radio = resource.NewModel("dtcurrie", "walkie", "radio")

func init() {
	resource.RegisterComponent(generic.API, Radio,
		resource.Registration[resource.Resource, *RadioConfig]{
			Constructor: newRadio,
		},
	)
}

// Default values for the optional attributes of RadioConfig.
const (
	defaultVoxThresholdDB  = -40.0
	defaultVoxHangoverMs   = 800
	defaultMaxQueuedChunks = 6
	defaultReconnectMs     = 1000

	// busyBackoff is how long a radio stops trying after a busy rejection: long
	// enough that a held talk button costs a few attempts a second, not fifty.
	busyBackoff = time.Second

	// revokedBackoff covers a floor taken back mid-transmission, which usually
	// means the hub is rebuilding. Shorter, because it is expected to clear.
	revokedBackoff = 250 * time.Millisecond
)

// RadioConfig is the JSON config for the radio model.
type RadioConfig struct {
	// Source is the local audio_in to listen to, and Sink the local audio_out
	// to play through.
	Source string `json:"source"`
	Sink   string `json:"sink"`

	// Uplink and Downlink name the hub endpoints. Both normally live on a
	// remote, so both must be the prefixed short name, e.g. "hub-uplink".
	Uplink   string `json:"uplink"`
	Downlink string `json:"downlink"`

	// Member is who this radio is on the network. It must be unique: it decides
	// who holds a channel, and it is what keeps a member from hearing their own
	// voice. Defaults to the component's own name.
	Member string `json:"member,omitempty"`

	// Channel is the channel to join at startup. Change it at runtime with
	// DoCommand {"channel": "..."} -- that is the whole point of the model, and
	// it needs no config edit.
	Channel string `json:"channel"`

	// GateMode is "manual" (push-to-talk, the default), "vox" (level-triggered)
	// or "open" (always on).
	GateMode string `json:"gate_mode,omitempty"`
	// StartTalking opens the manual gate as soon as the radio is built. Leave it
	// false on a shared channel, so this radio does not seize one the moment the
	// machine boots.
	StartTalking bool `json:"start_talking,omitempty"`

	VoxThresholdDBFS float64 `json:"vox_threshold_dbfs,omitempty"`
	VoxHangoverMs    int     `json:"vox_hangover_ms,omitempty"`

	// SampleRate and NumChannels are the format this machine's microphone is
	// expected to produce. On a channel that declares a format they must match
	// it, or the hub will refuse this radio's transmissions.
	SampleRate  int `json:"sample_rate,omitempty"`
	NumChannels int `json:"num_channels,omitempty"`

	MaxQueuedChunks int `json:"max_queued_chunks,omitempty"`
	ReconnectMs     int `json:"reconnect_ms,omitempty"`
}

// Validate checks the config and reports this resource's dependencies. All four
// endpoints are optional, so the radio still builds and reports which side is
// missing when the hub is asleep.
func (cfg *RadioConfig) Validate(path string) ([]string, []string, error) {
	for _, f := range []struct{ name, value string }{
		{"source", cfg.Source},
		{"sink", cfg.Sink},
		{"uplink", cfg.Uplink},
		{"downlink", cfg.Downlink},
		{"channel", cfg.Channel},
	} {
		if f.value == "" {
			return nil, nil, resource.NewConfigValidationFieldRequiredError(path, f.name)
		}
	}
	for _, f := range []struct{ name, value string }{
		{"source", cfg.Source},
		{"sink", cfg.Sink},
		{"uplink", cfg.Uplink},
		{"downlink", cfg.Downlink},
	} {
		if err := validateResourceRef(path, f.name, f.value); err != nil {
			return nil, nil, err
		}
	}

	if _, err := pump.ParseGateMode(cfg.GateMode); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := validateFormatPair(path, cfg.SampleRate, cfg.NumChannels); err != nil {
		return nil, nil, err
	}
	for _, f := range []struct {
		name  string
		value int
	}{
		{"vox_hangover_ms", cfg.VoxHangoverMs},
		{"max_queued_chunks", cfg.MaxQueuedChunks},
		{"reconnect_ms", cfg.ReconnectMs},
	} {
		if f.value < 0 {
			return nil, nil, fmt.Errorf("%s: %s must not be negative", path, f.name)
		}
	}

	return nil, []string{cfg.Source, cfg.Sink, cfg.Uplink, cfg.Downlink}, nil
}

// channelLister is the part of an endpoint a radio queries before retuning. The
// endpoint satisfies it in process and the generic client over the wire, so this
// works from either side.
type channelLister interface {
	DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error)
}

type radio struct {
	resource.AlwaysRebuild
	resource.Named

	logger logging.Logger
	cfg    *RadioConfig
	member string

	// Any of these is nil when the resource was not available at build time,
	// which is the normal state while the hub is asleep.
	source   audioin.AudioIn
	sink     audioout.AudioOut
	uplink   audioout.AudioOut
	downlink audioin.AudioIn

	// tx carries this machine's microphone to the hub; rx carries the channel
	// to this machine's speaker. Two pumps, opposite directions.
	tx, rx  *pump.Pump
	workers *goutils.StoppableWorkers

	// mu serialises retunes, so two concurrent DoCommands cannot interleave and
	// leave the two pumps on different channels.
	mu      sync.Mutex
	channel atomic.Pointer[string]

	retunes atomic.Uint64
	closed  atomic.Bool
}

var _ resource.Resource = (*radio)(nil)

// A switch drives a radio through DoCommand.
var _ channelLister = (*radio)(nil)

func newRadio(
	ctx context.Context,
	deps resource.Dependencies,
	rawConf resource.Config,
	logger logging.Logger,
) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*RadioConfig](rawConf)
	if err != nil {
		return nil, err
	}
	return NewRadio(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// NewRadio constructs a radio directly, without going through the registry.
func NewRadio(
	_ context.Context,
	deps resource.Dependencies,
	name resource.Name,
	conf *RadioConfig,
	logger logging.Logger,
) (resource.Resource, error) {
	gate, err := pump.ParseGateMode(conf.GateMode)
	if err != nil {
		return nil, err
	}

	member := conf.Member
	if member == "" {
		member = name.ShortName()
	}

	r := &radio{
		Named:  name.AsNamed(),
		logger: logger,
		cfg:    conf,
		member: member,
	}
	r.channel.Store(&conf.Channel)

	// Every endpoint is resolved leniently: a missing one is reported through
	// stats rather than failing construction.
	if res, err := deps.Lookup(audioin.Named(conf.Source)); err == nil {
		r.source, _ = res.(audioin.AudioIn)
	} else {
		logger.Warnw("microphone not available yet; this radio cannot talk until it is",
			"source", conf.Source, "error", err)
	}
	if res, err := deps.Lookup(audioout.Named(conf.Sink)); err == nil {
		r.sink, _ = res.(audioout.AudioOut)
	} else {
		logger.Warnw("speaker not available yet; this radio cannot listen until it is",
			"sink", conf.Sink, "error", err)
	}
	if res, err := deps.Lookup(audioout.Named(conf.Uplink)); err == nil {
		r.uplink, _ = res.(audioout.AudioOut)
	} else {
		logger.Warnw("hub uplink not available yet; is the hub remote awake?",
			"uplink", conf.Uplink, "error", err)
	}
	if res, err := deps.Lookup(audioin.Named(conf.Downlink)); err == nil {
		r.downlink, _ = res.(audioin.AudioIn)
	} else {
		logger.Warnw("hub downlink not available yet; is the hub remote awake?",
			"downlink", conf.Downlink, "error", err)
	}

	var expect *pcm.Format
	if conf.SampleRate > 0 && conf.NumChannels > 0 {
		expect = &pcm.Format{SampleRateHz: conf.SampleRate, NumChannels: conf.NumChannels}
	}

	extra := r.extraFor(conf.Channel)

	// The talking pump: this machine's microphone to the hub, gated.
	if r.source != nil && r.uplink != nil {
		r.tx = pump.New(r.source, r.uplink, pump.Config{
			GateMode:      gate,
			StartTalking:  conf.StartTalking,
			VoxThreshold:  orFloat(conf.VoxThresholdDBFS, defaultVoxThresholdDB),
			VoxHangover:   time.Duration(orInt(conf.VoxHangoverMs, defaultVoxHangoverMs)) * time.Millisecond,
			MaxQueued:     orInt(conf.MaxQueuedChunks, defaultMaxQueuedChunks),
			Reconnect:     time.Duration(orInt(conf.ReconnectMs, defaultReconnectMs)) * time.Millisecond,
			ExpectFormat:  expect,
			Extra:         extra,
			OnStreamError: classifyHubError,
			Logger:        logger.Sublogger("tx"),
		})
	}

	// The listening pump: the hub to this machine's speaker. Its gate is always
	// open -- gating stops a microphone feeding a speaker it can hear, and there is
	// no microphone on this side.
	if r.downlink != nil && r.sink != nil {
		r.rx = pump.New(r.downlink, r.sink, pump.Config{
			GateMode:      pump.GateOpen,
			MaxQueued:     orInt(conf.MaxQueuedChunks, defaultMaxQueuedChunks),
			Reconnect:     time.Duration(orInt(conf.ReconnectMs, defaultReconnectMs)) * time.Millisecond,
			Extra:         extra,
			OnStreamError: classifyHubError,
			Logger:        logger.Sublogger("rx"),
		})
	}

	// Neither pump may run on the constructor's goroutine: the audioin client
	// blocks on a Recv before GetAudio returns, so opening a stream here would
	// stall construction until somebody speaks.
	var work []func(context.Context)
	if r.tx != nil {
		work = append(work, r.tx.Run)
	}
	if r.rx != nil {
		work = append(work, r.rx.Run)
	}
	if len(work) > 0 {
		r.workers = goutils.NewBackgroundStoppableWorkers(work...)
	}

	logger.Infow("radio ready", "member", member, "channel", conf.Channel,
		"can_talk", r.tx != nil, "can_listen", r.rx != nil)
	return r, nil
}

// classifyHubError turns a hub rejection into a backoff. Without it a radio
// holding the talk button on a busy channel opens and loses a PlayStream per
// chunk, roughly fifty a second.
func classifyHubError(err error) (time.Duration, string) {
	switch {
	case bus.IsBusy(err):
		return busyBackoff, "channel busy: somebody else is talking"
	case bus.IsRevoked(err):
		return revokedBackoff, "the hub took the channel back mid-transmission"
	case status.Code(err) == codes.NotFound:
		// A channel that vanished under us. Worth backing off hard: retrying
		// cannot fix a channel the hub does not declare.
		return 5 * time.Second, "the hub does not carry this channel"
	default:
		return 0, ""
	}
}

func (r *radio) extraFor(channel string) map[string]interface{} {
	return map[string]interface{}{ChannelKey: channel, MemberKey: r.member}
}

// Channel reports which channel this radio is tuned to.
func (r *radio) Channel() string {
	if c := r.channel.Load(); c != nil {
		return *c
	}
	return ""
}

var errRadioClosed = errors.New("this radio has been closed, which on a running machine means it " +
	"is being rebuilt; it will be back shortly")

// retune moves both pumps to a new channel without a config change.
func (r *radio) retune(ctx context.Context, channel string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if channel == r.Channel() {
		return nil
	}
	if err := r.checkChannelExists(ctx, channel); err != nil {
		return err
	}

	extra := r.extraFor(channel)
	if r.tx != nil {
		r.tx.SetExtra(extra)
		// End the current transmission so the floor on the old channel is
		// released now, rather than held until its hangover expires.
		r.tx.ResetSink()
	}
	if r.rx != nil {
		r.rx.SetExtra(extra)
		// The listening stream is long-lived, so it has to be cut for the new
		// channel to take effect at all.
		r.rx.ResetSource()
	}
	r.channel.Store(&channel)
	r.retunes.Add(1)

	r.logger.Infow("retuned", "member", r.member, "channel", channel)
	return nil
}

// checkChannelExists refuses a bad retune up front. Left to the async path,
// tuning to a channel the hub does not carry is silent deafness with the reason
// buried in a stat.
func (r *radio) checkChannelExists(ctx context.Context, channel string) error {
	var lister channelLister
	switch {
	case r.uplink != nil:
		lister = r.uplink
	case r.downlink != nil:
		lister = r.downlink
	default:
		// Nothing to ask. Allow it: the radio is already reporting that it
		// cannot reach the hub, and refusing here would make a disconnected
		// radio impossible to pre-tune.
		return nil
	}

	resp, err := lister.DoCommand(ctx, map[string]interface{}{"channels": true})
	if err != nil {
		r.logger.Debugw("could not check the channel list before retuning; allowing it",
			"channel", channel, "error", err)
		return nil
	}

	known, ok := resp["channels"].([]interface{})
	if !ok {
		return nil
	}
	names := make([]string, 0, len(known))
	for _, k := range known {
		if s, ok := k.(string); ok {
			if s == channel {
				return nil
			}
			names = append(names, s)
		}
	}
	return fmt.Errorf("the hub does not carry a channel called %q; it carries %v", channel, names)
}

// DoCommand is the radio's whole control surface. Keys are applied in a fixed
// order within one call -- retune, gate mode, talk -- so a combined command keys
// up on the new channel, not the old.
func (r *radio) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if r.closed.Load() {
		return nil, errRadioClosed
	}
	if len(cmd) == 0 {
		return nil, errors.New(`empty command; try {"stats": true}`)
	}

	out := map[string]interface{}{}
	handled := false

	if v, ok := cmd[ChannelKey]; ok {
		channel, err := stringArg(ChannelKey, v)
		if err != nil {
			return nil, err
		}
		if err := r.retune(ctx, channel); err != nil {
			return nil, err
		}
		out[ChannelKey] = channel
		handled = true
	}

	if v, ok := cmd["gate_mode"]; ok {
		s, err := stringArg("gate_mode", v)
		if err != nil {
			return nil, err
		}
		mode, err := pump.ParseGateMode(s)
		if err != nil {
			return nil, err
		}
		if r.tx == nil {
			return nil, errors.New("this radio cannot talk: its microphone or the hub uplink is missing")
		}
		r.tx.SetGateMode(mode)
		out["gate_mode"] = mode.String()
		handled = true
	}

	if v, ok := cmd["talk"]; ok {
		on, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf(`"talk" must be a bool, got %T`, v)
		}
		if r.tx == nil {
			return nil, errors.New("this radio cannot talk: its microphone or the hub uplink is missing")
		}
		if seconds, ok := cmd["seconds"]; ok && on {
			d, err := numberArg("seconds", seconds)
			if err != nil {
				return nil, err
			}
			r.tx.SetTalkingFor(time.Duration(d * float64(time.Second)))
			out["seconds"] = d
		} else {
			r.tx.SetTalking(on)
		}
		out["talking"] = r.tx.Talking()
		handled = true
	}

	if v, ok := cmd["stats"]; ok && truthy(v) {
		for k, val := range r.status() {
			out[k] = val
		}
		handled = true
	}

	if !handled {
		return nil, fmt.Errorf(`unrecognised command; try {"stats": true}, {"talk": true}, `+
			`{"gate_mode": "vox"} or {"channel": "ops"}, got %v`, keysOf(cmd))
	}
	return out, nil
}

// Status reports the radio's state for the machine page.
func (r *radio) Status(context.Context) (map[string]interface{}, error) {
	return r.status(), nil
}

// status is the JSON-safe snapshot behind both Status and {"stats": true}. Every
// number is cast for the structpb round trip, and a missing endpoint's counters
// are omitted rather than reported as a zero.
func (r *radio) status() map[string]interface{} {
	out := map[string]interface{}{
		"member":             r.member,
		"channel":            r.Channel(),
		"source":             r.cfg.Source,
		"sink":               r.cfg.Sink,
		"uplink":             r.cfg.Uplink,
		"downlink":           r.cfg.Downlink,
		"source_available":   r.source != nil,
		"sink_available":     r.sink != nil,
		"uplink_available":   r.uplink != nil,
		"downlink_available": r.downlink != nil,
		"can_talk":           r.tx != nil,
		"can_listen":         r.rx != nil,
		"ready":              r.tx != nil && r.rx != nil,
		"retunes":            int64(r.retunes.Load()),
	}
	if r.closed.Load() {
		out["ready"] = false
		return out
	}

	if r.tx != nil {
		s := r.tx.Stats()
		out["gate_open"] = s.GateOpen
		out["gate_mode"] = s.GateMode
		out["talking"] = r.tx.Talking()
		out["tx_connected"] = s.Connected
		out["tx_transmissions"] = int64(s.Transmissions)
		out["tx_chunks_in"] = int64(s.ChunksIn)
		out["tx_chunks_out"] = int64(s.ChunksOut)
		out["tx_chunks_dropped"] = int64(s.ChunksDropped)
		out["tx_bytes_out"] = int64(s.BytesOut)
		out["tx_reconnects"] = int64(s.Reconnects)
		out["tx_format_mismatch"] = int64(s.FormatMismatch)
		out["tx_format_unexpected"] = int64(s.FormatUnexpected)
		out["tx_peak_dbfs"] = s.PeakDBFS
		out["tx_silent_seconds"] = s.SilentSeconds
		out["tx_last_chunk_age_ms"] = s.LastChunkAge.Milliseconds()
		// Its own counter, never derived from the chunk counters: the audioout client
		// sends its header without waiting for an ack, so a rejected transmission gets
		// one chunk out before the refusal arrives.
		out["busy_rejections"] = int64(s.Suppressed)
		if s.LastErr != "" {
			out["tx_last_error"] = s.LastErr
		}
	}

	if r.rx != nil {
		s := r.rx.Stats()
		out["rx_connected"] = s.Connected
		out["rx_streams"] = int64(s.Streams)
		out["rx_chunks_in"] = int64(s.ChunksIn)
		out["rx_chunks_out"] = int64(s.ChunksOut)
		out["rx_chunks_dropped"] = int64(s.ChunksDropped)
		out["rx_reconnects"] = int64(s.Reconnects)
		out["rx_peak_dbfs"] = s.PeakDBFS
		out["rx_last_chunk_age_ms"] = s.LastChunkAge.Milliseconds()
		// The hub sends an empty chunk on a cadence, so this is how a quiet
		// channel is told apart from a hub that has stopped answering.
		out["hub_heartbeat_age_ms"] = s.LastHeartbeatAge.Milliseconds()
		if s.LastErr != "" {
			out["rx_last_error"] = s.LastErr
		}
	}

	return out
}

func (r *radio) Close(context.Context) error {
	r.closed.Store(true)
	if r.workers != nil {
		r.workers.Stop()
	}
	return nil
}

// numberArg accepts the shapes a number can arrive in through structpb, which
// turns every JSON number into a float64 but is not the only caller.
func numberArg(key string, v interface{}) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("%q must be a number, got %T", key, v)
	}
}

func orInt(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func orFloat(v, fallback float64) float64 {
	if v == 0 {
		return fallback
	}
	return v
}
