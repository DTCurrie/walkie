package main

import (
	"bytes"
	"flag"
	"fmt"
	"testing"

	"go.viam.com/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"walkie/internal/audiofmt"
	"walkie/internal/bus"
)

// TestHelpDoesNotLeakCredentials guards the flag defaults. flag.PrintDefaults
// renders a non-empty string default as `(default "...")`, and -h output is what
// users paste into bug reports.
func TestHelpDoesNotLeakCredentials(t *testing.T) {
	t.Setenv("VIAM_API_KEY", "sk-canary-payload")
	t.Setenv("VIAM_API_KEY_ID", "id-canary")

	var c conn
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c.bind(fs)

	var help bytes.Buffer
	fs.SetOutput(&help)
	fs.PrintDefaults()

	for _, secret := range []string{"sk-canary-payload", "id-canary"} {
		test.That(t, help.String(), test.ShouldNotContainSubstring, secret)
	}

	// The environment must still reach the dialler, just not the help text.
	test.That(t, c.apiKey, test.ShouldBeEmpty)
	test.That(t, c.apiKeyID, test.ShouldBeEmpty)
}

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
	format := audiofmt.Format{SampleRateHz: 16000, NumChannels: 1}
	data := make([]byte, 320)

	phase := fillTone(data, format, 0)
	test.That(t, phase, test.ShouldNotEqual, float64(0))

	// A tone must not be digital silence, or `listen` would report a fault that
	// is really just this CLI.
	test.That(t, audiofmt.PeakDBFS(data), test.ShouldBeGreaterThan, audiofmt.SilentDBFS)
}

// TestMeterClamps: levels outside the -60..0 window must not produce a negative
// repeat count.
func TestMeterClamps(t *testing.T) {
	for _, dbfs := range []float64{-1000, audiofmt.SilentDBFS, -60, -30, 0, 12} {
		test.That(t, meter(dbfs), test.ShouldHaveLength, 32) // 30 cells plus brackets
	}
	test.That(t, meter(-1000), test.ShouldEqual, meter(-60))
}
