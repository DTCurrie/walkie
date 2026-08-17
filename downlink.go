package walkie

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/bus"
)

// Downlink is the model triple for the hub endpoint members listen to.
var Downlink = resource.NewModel("dtcurrie", "walkie", "downlink")

func init() {
	resource.RegisterComponent(audioin.API, Downlink,
		resource.Registration[audioin.AudioIn, *DownlinkConfig]{
			Constructor: newDownlink,
		},
	)
}

// DownlinkConfig is the JSON config for the downlink model.
type DownlinkConfig struct {
	// Bus names the dtcurrie:walkie:bus this endpoint reads from. It must be on
	// the same machine and in the same module.
	Bus string `json:"bus"`

	// Channel and Member are defaults for callers that do not set them in
	// extra, exactly as on the uplink.
	Channel string `json:"channel,omitempty"`
	Member  string `json:"member,omitempty"`
}

// Validate checks the config and reports the bus as a required dependency.
func (cfg *DownlinkConfig) Validate(path string) ([]string, []string, error) {
	if cfg.Bus == "" {
		return nil, nil, resource.NewConfigValidationFieldRequiredError(path, "bus")
	}
	if err := refs.ResourceRef(path, "bus", cfg.Bus); err != nil {
		return nil, nil, err
	}
	return []string{cfg.Bus}, nil, nil
}

type downlink struct {
	resource.AlwaysRebuild
	resource.Named

	logger logging.Logger
	cfg    *DownlinkConfig
	bus    *bus.Bus
	closed atomic.Bool
}

var _ audioin.AudioIn = (*downlink)(nil)

func newDownlink(
	_ context.Context,
	deps resource.Dependencies,
	rawConf resource.Config,
	logger logging.Logger,
) (audioin.AudioIn, error) {
	conf, err := resource.NativeConfig[*DownlinkConfig](rawConf)
	if err != nil {
		return nil, err
	}
	router, err := resolveBus(deps, conf.Bus)
	if err != nil {
		return nil, err
	}
	return &downlink{
		Named:  rawConf.ResourceName().AsNamed(),
		logger: logger,
		cfg:    conf,
		bus:    router,
	}, nil
}

// GetAudio subscribes the caller to their channel, which closes exactly once and
// is ranged to completion, as the bus assumes. previousTimestampNs is ignored:
// a live channel has no backlog.
func (d *downlink) GetAudio(ctx context.Context, codec string, durationSeconds float32,
	_ int64, extra map[string]interface{},
) (chan *audioin.AudioChunk, error) {
	if d.closed.Load() {
		return nil, status.Error(codes.Unavailable, errBusClosed.Error())
	}

	id, err := resolveIdentity(extra, d.cfg.Channel, d.cfg.Member)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var duration time.Duration
	if durationSeconds > 0 {
		duration = time.Duration(float64(durationSeconds) * float64(time.Second))
	}

	out, cancel, err := d.bus.Subscribe(ctx, bus.SubReq{
		Channel:  id.Channel,
		Member:   id.Member,
		Codec:    codec,
		Duration: duration,
	})
	if err != nil {
		return nil, bus.ToStatus(err)
	}

	// The bus stops the subscription on ctx anyway; this is belt and braces for
	// the case where the server returns without cancelling.
	context.AfterFunc(ctx, cancel)

	d.logger.Debugw("listener joined", "channel", id.Channel, "member", id.Member,
		"duration", duration.String())
	return out, nil
}

// Properties reports what this endpoint delivers.
func (d *downlink) Properties(_ context.Context, extra map[string]interface{}) (rutils.Properties, error) {
	if d.closed.Load() {
		return rutils.Properties{}, status.Error(codes.Unavailable, errBusClosed.Error())
	}

	// A hub with several channels at several formats has no single truthful
	// answer, so answer for the caller's channel when they name one.
	format := d.bus.DefaultFormat()
	if id, err := resolveIdentity(extra, d.cfg.Channel, d.cfg.Member); err == nil {
		if f, ok := d.bus.ChannelFormat(id.Channel); ok {
			format = f
		}
	}
	return rutils.Properties{
		SupportedCodecs: []string{rutils.CodecPCM16},
		SampleRateHz:    int32(format.SampleRateHz),
		NumChannels:     int32(format.NumChannels),
	}, nil
}

// DoCommand answers the same channel query the uplink does, so a radio can ask
// whichever endpoint it can reach.
func (d *downlink) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if d.closed.Load() {
		return nil, errBusClosed
	}
	if v, ok := cmd["channels"]; ok && truthy(v) {
		return map[string]interface{}{"channels": toAnySlice(d.bus.Channels())}, nil
	}
	return nil, errors.New(`unrecognised command; try {"channels": true}`)
}

func (d *downlink) Close(context.Context) error {
	// The bus belongs to the bus component, not to us.
	d.closed.Store(true)
	return nil
}
