package walkie

import (
	"errors"
	"fmt"
	"strings"

	"github.com/DTCurrie/viam-comms/viamcfg"
)

// The keys a caller uses to say which channel it wants and who it is. They
// travel in the extra map, which both audio APIs deliver per-caller -- what lets
// one pair of hub endpoints serve every channel.
const (
	ChannelKey = "channel"
	MemberKey  = "member"
)

// identity is who is calling, and about which channel.
type identity struct {
	Channel string
	Member  string
}

// resolveIdentity reads the caller's identity out of extra, falling back to the
// endpoint's configured defaults. Defaults let an endpoint serve one channel,
// driven by a caller that cannot set extra.
func resolveIdentity(extra map[string]interface{}, defChannel, defMember string) (identity, error) {
	id := identity{Channel: defChannel, Member: defMember}

	if v, ok := extra[ChannelKey]; ok {
		s, err := stringArg(ChannelKey, v)
		if err != nil {
			return identity{}, err
		}
		id.Channel = s
	}
	if v, ok := extra[MemberKey]; ok {
		s, err := stringArg(MemberKey, v)
		if err != nil {
			return identity{}, err
		}
		id.Member = s
	}

	if id.Channel == "" {
		return identity{}, fmt.Errorf(
			"no channel: pass %q in extra, or give this endpoint a %q attribute to default to",
			ChannelKey, ChannelKey)
	}
	if id.Member == "" {
		// Refused rather than defaulted, because an anonymous talker cannot be
		// kept out of their own speaker: self-echo suppression matches on this
		// name, and a blank one would match every other blank one.
		return identity{}, fmt.Errorf(
			"no member: pass %q in extra, or give this endpoint a %q attribute. "+
				"It identifies who holds the channel and who must not hear their own voice",
			MemberKey, MemberKey)
	}
	return id, nil
}

func stringArg(key string, v interface{}) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%q must be a string, got %T", key, v)
	}
	if strings.TrimSpace(s) != s {
		return "", fmt.Errorf("%q must not have leading or trailing spaces, got %q", key, s)
	}
	return s, nil
}

// errNotLocalBus explains the one wiring mistake the config cannot reveal. Only
// a dependency in the same module on the same machine arrives as the concrete Go
// object; anything else is a gRPC client.
var errNotLocalBus = errors.New(
	"this endpoint could not reach its bus as a local object. The bus, the uplink and " +
		"the downlink must all be configured on the same machine, because they share " +
		"routing state in process rather than over the network. Check that the \"bus\" " +
		"attribute names a dtcurrie:walkie:bus on this part, not one on a remote")

// refs quotes walkie-flavoured examples in the colon-rule error.
var refs = viamcfg.Validator{ExamplePrefix: "hub-", ExampleName: "hub-uplink"}
