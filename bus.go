// Package walkie is a channel-based audio network for Viam machines. One machine
// is the hub, running a bus and two endpoints; every other adds a remote and one
// radio. The routing lives in internal/bus.
package walkie

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	goutils "go.viam.com/utils"

	"walkie/internal/audiofmt"
	"walkie/internal/bus"
)

// Bus is the model triple for this module's channel bus.
var Bus = resource.NewModel("dtcurrie", "walkie", "bus")

func init() {
	resource.RegisterComponent(generic.API, Bus,
		resource.Registration[resource.Resource, *BusConfig]{
			Constructor: newWalkieBus,
		},
	)
}

// Default values for the optional attributes of BusConfig.
const (
	defaultSampleRate  = 16000
	defaultNumChannels = 1
)

// BusConfig is the JSON config for the bus model.
type BusConfig struct {
	// Channels declares every channel this hub carries. A member may only tune
	// to a channel named here.
	Channels []ChannelConfig `json:"channels"`

	// SampleRate and NumChannels are the format for channels that do not declare
	// one. The default is 16kHz mono: the hub copies every talker to every
	// listener, so 48kHz would be ~7 Mbit/s off one machine.
	SampleRate  int `json:"sample_rate,omitempty"`
	NumChannels int `json:"num_channels,omitempty"`

	// MaxQueuedChunks bounds how much audio may wait for one listener before
	// the oldest is discarded. Keep it small; it is latency, not safety.
	MaxQueuedChunks int `json:"max_queued_chunks,omitempty"`

	// FloorHangoverMs is how long a talker who stops keeps the right to carry
	// on before anyone else may take the channel.
	FloorHangoverMs int `json:"floor_hangover_ms,omitempty"`

	// FloorIdleMs is how long a transmission may send nothing before the hub takes
	// its channel back. This is what recovers a channel from a member that lost
	// power, which nothing else can detect.
	FloorIdleMs int `json:"floor_idle_ms,omitempty"`

	// KeepaliveMs is how often an idle listener is sent an empty chunk, so a
	// quiet channel stays distinguishable from a hub that has stopped. Set it
	// to -1 to disable.
	KeepaliveMs int `json:"keepalive_ms,omitempty"`
}

// ChannelConfig declares one channel.
type ChannelConfig struct {
	Name string `json:"name"`
	// SampleRate and NumChannels, when both set, are enforced: a talker in any
	// other format is refused. Nothing here resamples, so the alternative to
	// refusing one talker is garbled audio at every listener.
	SampleRate  int `json:"sample_rate,omitempty"`
	NumChannels int `json:"num_channels,omitempty"`
}

// Validate checks the config. A bus has no dependencies: it owns the channels
// and the endpoints depend on it, never the other way round.
func (cfg *BusConfig) Validate(path string) ([]string, []string, error) {
	if len(cfg.Channels) == 0 {
		return nil, nil, fmt.Errorf("%s: needs at least one entry in %q", path, "channels")
	}

	seen := make(map[string]bool, len(cfg.Channels))
	for i, ch := range cfg.Channels {
		where := fmt.Sprintf("%s: channels[%d]", path, i)
		if ch.Name == "" {
			return nil, nil, fmt.Errorf("%s: needs a %q", where, "name")
		}
		if seen[ch.Name] {
			return nil, nil, fmt.Errorf("%s: channel %q is listed more than once", where, ch.Name)
		}
		seen[ch.Name] = true

		if err := validateFormatPair(where, ch.SampleRate, ch.NumChannels); err != nil {
			return nil, nil, err
		}
	}

	if err := validateFormatPair(path, cfg.SampleRate, cfg.NumChannels); err != nil {
		return nil, nil, err
	}
	for _, f := range []struct {
		name  string
		value int
	}{
		{"max_queued_chunks", cfg.MaxQueuedChunks},
		{"floor_hangover_ms", cfg.FloorHangoverMs},
		{"floor_idle_ms", cfg.FloorIdleMs},
	} {
		if f.value < 0 {
			return nil, nil, fmt.Errorf("%s: %s must not be negative", path, f.name)
		}
	}
	if hangover := time.Duration(cfg.FloorHangoverMs) * time.Millisecond; cfg.FloorHangoverMs > 0 &&
		hangover < bus.MinFloorHangover {
		return nil, nil, fmt.Errorf(
			"%s: floor_hangover_ms of %d is shorter than the %s a talker's own stream takes "+
				"to close, so it would expire before the previous transmission had finished "+
				"and let a bystander take the channel mid-sentence",
			path, cfg.FloorHangoverMs, bus.MinFloorHangover)
	}

	return nil, nil, nil
}

func validateFormatPair(path string, rate, channels int) error {
	if rate == 0 && channels == 0 {
		return nil
	}
	if rate == 0 || channels == 0 {
		return fmt.Errorf("%s: set both sample_rate and num_channels, or neither", path)
	}
	if err := (audiofmt.Format{SampleRateHz: rate, NumChannels: channels}).Valid(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// busHolder is what the endpoints need from a bus. Keeping it to an interface
// means the endpoint tests can drive a fake without standing up a real one.
type busHolder interface {
	router() *bus.Bus
}

type walkieBus struct {
	resource.AlwaysRebuild
	resource.Named

	logger  logging.Logger
	b       *bus.Bus
	workers *goutils.StoppableWorkers
	closed  atomic.Bool
}

var _ resource.Resource = (*walkieBus)(nil)
var _ busHolder = (*walkieBus)(nil)

func newWalkieBus(
	ctx context.Context,
	_ resource.Dependencies,
	rawConf resource.Config,
	logger logging.Logger,
) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*BusConfig](rawConf)
	if err != nil {
		return nil, err
	}
	return NewBus(ctx, rawConf.ResourceName(), conf, logger)
}

// NewBus constructs a bus directly, without going through the registry.
func NewBus(
	_ context.Context,
	name resource.Name,
	conf *BusConfig,
	logger logging.Logger,
) (resource.Resource, error) {
	format := audiofmt.Format{SampleRateHz: defaultSampleRate, NumChannels: defaultNumChannels}
	if conf.SampleRate > 0 && conf.NumChannels > 0 {
		format = audiofmt.Format{SampleRateHz: conf.SampleRate, NumChannels: conf.NumChannels}
	}

	channels := make([]bus.ChannelConfig, 0, len(conf.Channels))
	for _, ch := range conf.Channels {
		channels = append(channels, bus.ChannelConfig{
			Name:        ch.Name,
			SampleRate:  ch.SampleRate,
			NumChannels: ch.NumChannels,
		})
	}

	keepalive := time.Duration(conf.KeepaliveMs) * time.Millisecond
	if conf.KeepaliveMs < 0 {
		// Negative is the explicit "off", since zero has to mean "unset" for
		// every other duration in this config.
		keepalive = -1
	}

	router, err := bus.New(bus.Config{
		Channels:      channels,
		MaxQueued:     conf.MaxQueuedChunks,
		FloorHangover: time.Duration(conf.FloorHangoverMs) * time.Millisecond,
		FloorIdle:     time.Duration(conf.FloorIdleMs) * time.Millisecond,
		Keepalive:     keepalive,
		DefaultFormat: format,
		Logger:        logger,
	})
	if err != nil {
		return nil, err
	}

	wb := &walkieBus{
		Named:  name.AsNamed(),
		logger: logger,
		b:      router,
	}
	wb.workers = goutils.NewBackgroundStoppableWorkers(router.Run)

	logger.Infow("channel bus ready", "channels", router.Channels(), "format", format.String())
	return wb, nil
}

func (w *walkieBus) router() *bus.Bus { return w.b }

var errBusClosed = errors.New("this bus has been closed, which on a running machine means it is " +
	"being rebuilt; it will be back shortly")

// DoCommand exposes the bus's state. Everything it returns is JSON-safe, since
// it crosses a structpb round trip.
func (w *walkieBus) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if w.closed.Load() {
		return nil, errBusClosed
	}
	if len(cmd) == 0 {
		return nil, errors.New(`empty command; try {"stats": true}`)
	}

	out := map[string]interface{}{}
	handled := false

	if v, ok := cmd["channels"]; ok && truthy(v) {
		out["channels"] = toAnySlice(w.b.Channels())
		handled = true
	}

	if name, ok := cmd["clear_floor"]; ok {
		s, err := stringArg("clear_floor", name)
		if err != nil {
			return nil, err
		}
		holder, err := w.b.ClearFloor(s)
		if err != nil {
			return nil, bus.ToStatus(err)
		}
		out["cleared"] = s
		out["was_holding"] = holder
		handled = true
	}

	if v, ok := cmd["stats"]; ok && truthy(v) {
		out["channels_detail"] = w.statsPayload()
		handled = true
	}

	if !handled {
		return nil, fmt.Errorf(`unrecognised command; try {"stats": true}, {"channels": true} `+
			`or {"clear_floor": "<channel>"}, got %v`, keysOf(cmd))
	}
	return out, nil
}

func (w *walkieBus) statsPayload() []interface{} {
	stats := w.b.Stats()
	out := make([]interface{}, 0, len(stats.Channels))
	for _, cs := range stats.Channels {
		out = append(out, map[string]interface{}{
			"name":            cs.Name,
			"format":          cs.Format,
			"listeners":       int64(cs.Listeners),
			"members":         toAnySlice(cs.Members),
			"holder":          cs.Holder,
			"transmissions":   int64(cs.Transmissions),
			"busy_rejections": int64(cs.BusyRejections),
			"revocations":     int64(cs.Revocations),
			"chunks_sent":     int64(cs.ChunksSent),
			"chunks_dropped":  int64(cs.ChunksDropped),
			"keepalives":      int64(cs.Keepalives),
		})
	}
	return out
}

// Status reports the same picture as DoCommand's stats, for the machine page.
func (w *walkieBus) Status(_ context.Context) (map[string]interface{}, error) {
	if w.closed.Load() {
		return map[string]interface{}{"ready": false}, nil
	}
	return map[string]interface{}{
		"ready":           true,
		"channels":        toAnySlice(w.b.Channels()),
		"channels_detail": w.statsPayload(),
	}, nil
}

func (w *walkieBus) Close(context.Context) error {
	w.closed.Store(true)
	if w.workers != nil {
		w.workers.Stop()
	}
	return w.b.Close()
}

// truthy accepts the shapes a bool can arrive in through structpb.
func truthy(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true"
	case float64:
		return t != 0
	default:
		return false
	}
}

func toAnySlice(in []string) []interface{} {
	out := make([]interface{}, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	return out
}

func keysOf(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
