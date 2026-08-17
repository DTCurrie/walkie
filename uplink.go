package walkie

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DTCurrie/viam-comms/audio/pcm"
	"walkie/internal/bus"
)

// Uplink is the model triple for the hub endpoint members talk into.
var Uplink = resource.NewModel("dtcurrie", "walkie", "uplink")

func init() {
	resource.RegisterComponent(audioout.API, Uplink,
		resource.Registration[audioout.AudioOut, *UplinkConfig]{
			Constructor: newUplink,
		},
	)
}

// UplinkConfig is the JSON config for the uplink model.
type UplinkConfig struct {
	// Bus names the dtcurrie:walkie:bus this endpoint feeds. It must be on the
	// same machine and in the same module.
	Bus string `json:"bus"`

	// Channel and Member are defaults for callers that do not set them in
	// extra. A radio always sets both, so these are only needed to dedicate an
	// endpoint to one member on one channel.
	Channel string `json:"channel,omitempty"`
	Member  string `json:"member,omitempty"`
}

// Validate checks the config and reports the bus as a required dependency.
// Required, unlike a talkback link's endpoints: the bus is an in-process object
// here, and an uplink without one can do nothing.
func (cfg *UplinkConfig) Validate(path string) ([]string, []string, error) {
	if cfg.Bus == "" {
		return nil, nil, resource.NewConfigValidationFieldRequiredError(path, "bus")
	}
	if err := refs.ResourceRef(path, "bus", cfg.Bus); err != nil {
		return nil, nil, err
	}
	return []string{cfg.Bus}, nil, nil
}

type uplink struct {
	resource.AlwaysRebuild
	resource.Named

	logger logging.Logger
	cfg    *UplinkConfig
	bus    *bus.Bus
	closed atomic.Bool
}

var _ audioout.AudioOut = (*uplink)(nil)

func newUplink(
	ctx context.Context,
	deps resource.Dependencies,
	rawConf resource.Config,
	logger logging.Logger,
) (audioout.AudioOut, error) {
	conf, err := resource.NativeConfig[*UplinkConfig](rawConf)
	if err != nil {
		return nil, err
	}
	router, err := resolveBus(deps, conf.Bus)
	if err != nil {
		return nil, err
	}
	return &uplink{
		Named:  rawConf.ResourceName().AsNamed(),
		logger: logger,
		cfg:    conf,
		bus:    router,
	}, nil
}

// resolveBus finds the bus and insists on getting the real object rather than
// an RPC client standing in for one.
func resolveBus(deps resource.Dependencies, name string) (*bus.Bus, error) {
	res, err := deps.Lookup(generic.Named(name))
	if err != nil {
		return nil, err
	}
	holder, ok := res.(busHolder)
	if !ok {
		return nil, fmt.Errorf("%q: %w", name, errNotLocalBus)
	}
	return holder.router(), nil
}

// PlayStream carries one member's transmission. It must either drain chunks to
// close and return nil, or return non-nil: a nil return without draining wedges
// the talker, so early returns are all non-nil.
func (u *uplink) PlayStream(ctx context.Context, info *rutils.AudioInfo,
	chunks <-chan []byte, extra map[string]interface{},
) error {
	if u.closed.Load() {
		return status.Error(codes.Unavailable, errBusClosed.Error())
	}

	id, err := resolveIdentity(extra, u.cfg.Channel, u.cfg.Member)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	format, err := pcm.FromAudioInfo(info)
	if err != nil {
		return status.Errorf(codes.InvalidArgument,
			"a transmission must declare its format in PlayStreamInit: %v", err)
	}

	tx, err := u.bus.Publish(ctx, bus.TxReq{
		Channel: id.Channel,
		Member:  id.Member,
		Format:  format,
		Info:    info,
	})
	if err != nil {
		// Returning here without draining is legal precisely because this is a
		// non-nil return.
		return bus.ToStatus(err)
	}
	defer tx.Close()

	u.logger.Debugw("transmission opened", "channel", id.Channel, "member", id.Member,
		"format", format.String())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-tx.Revoked():
			// The floor was taken back: the bus is shutting down, an operator
			// cleared it, or the watchdog decided this talker had gone away.
			return status.Error(codes.Aborted, bus.RevokedMsg)

		case data, ok := <-chunks:
			if !ok {
				// Drained to close. This is the one path that returns nil.
				return nil
			}
			if err := tx.Send(data); err != nil {
				return bus.ToStatus(err)
			}
		}
	}
}

// Play refuses, and says what to do instead. It takes a complete buffer, the
// record-then-send model this module exists to avoid, and would make floor
// control meaningless.
func (u *uplink) Play(context.Context, []byte, *rutils.AudioInfo, map[string]interface{}) error {
	return status.Error(codes.Unimplemented,
		"a walkie uplink carries live transmissions, so it serves PlayStream and not Play. "+
			"Play would require the whole utterance up front, which defeats both the point "+
			"of the module and the channel floor")
}

// Properties reports what a talker should send. With channels carrying different
// formats there is no single truthful answer, so this reports the default and
// leaves enforcement to Publish.
func (u *uplink) Properties(context.Context, map[string]interface{}) (rutils.Properties, error) {
	if u.closed.Load() {
		return rutils.Properties{}, status.Error(codes.Unavailable, errBusClosed.Error())
	}
	return u.busProperties(), nil
}

func (u *uplink) busProperties() rutils.Properties {
	format := u.bus.DefaultFormat()
	if u.cfg.Channel != "" {
		if f, ok := u.bus.ChannelFormat(u.cfg.Channel); ok {
			format = f
		}
	}
	return rutils.Properties{
		SupportedCodecs: []string{rutils.CodecPCM16},
		SampleRateHz:    int32(format.SampleRateHz),
		NumChannels:     int32(format.NumChannels),
	}
}

// DoCommand lets a member ask what channels exist before tuning to one, which
// is how a radio rejects a bad retune synchronously instead of going quietly
// deaf.
func (u *uplink) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if u.closed.Load() {
		return nil, errBusClosed
	}
	if v, ok := cmd["channels"]; ok && truthy(v) {
		return map[string]interface{}{"channels": toAnySlice(u.bus.Channels())}, nil
	}
	return nil, errors.New(`unrecognised command; try {"channels": true}`)
}

func (u *uplink) Close(context.Context) error {
	// The bus is not ours to close: it belongs to the bus component, and the
	// endpoints are rebuilt alongside it.
	u.closed.Store(true)
	return nil
}
