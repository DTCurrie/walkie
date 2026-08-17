package main

import (
	"fmt"
	"testing"

	"go.viam.com/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DTCurrie/viam-comms/audio/pcm"
	"walkie/internal/bus"
)

func TestIdentityFlagsRequireAChannel(t *testing.T) {
	var id identityFlags
	_, err := id.extra()
	test.That(t, err, test.ShouldNotBeNil)

	id.channel = "ops"
	id.member = "probe"
	extra, err := id.extra()
	test.That(t, err, test.ShouldBeNil)
	test.That(t, extra, test.ShouldResemble, map[string]interface{}{
		"channel": "ops", "member": "probe",
	})
}

// TestDescribeTalkErrorIsActionable: the raw error crosses several client and
// server hops and arrives close to unreadable, so the CLI translates the ones an
// operator will actually hit. It matches on gRPC codes, never on message text.
func TestDescribeTalkErrorIsActionable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "busy",
			err:  bus.ToStatus(bus.BusyError("alpha")),
			want: "wait for them",
		},
		{
			name: "unknown channel",
			err:  bus.ToStatus(bus.UnknownChannelError("nope", []string{"ops"})),
			want: "walkie-cli channels",
		},
		{
			name: "format rejected",
			err:  bus.ToStatus(fmt.Errorf("%w: channel carries 16000Hz/1ch", bus.ErrFormat)),
			want: "--rate",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := describeTalkError(tc.err, "ops")
			test.That(t, got, test.ShouldNotBeNil)
			test.That(t, got.Error(), test.ShouldContainSubstring, tc.want)
		})
	}

	test.That(t, describeTalkError(nil, "ops"), test.ShouldBeNil)

	// Anything unrecognised is passed through rather than mislabelled.
	other := status.Error(codes.Internal, "speaker on fire")
	test.That(t, describeTalkError(other, "ops"), test.ShouldEqual, other)
}

// TestFillToneIsContinuous: the phase has to carry across chunks, or every chunk
// boundary is a click that looks like a fault.
func TestFillToneIsContinuous(t *testing.T) {
	format := pcm.Format{SampleRateHz: 16000, NumChannels: 1}
	data := make([]byte, 320)

	phase := fillTone(data, format, 0)
	test.That(t, phase, test.ShouldNotEqual, float64(0))

	// A tone must not be digital silence, or `listen` would report a fault that
	// is really just this CLI.
	test.That(t, pcm.PeakDBFS(data), test.ShouldBeGreaterThan, pcm.SilentDBFS)
}
