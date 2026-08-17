package audiofmt

import (
	"math"
	"testing"
	"time"

	rutils "go.viam.com/rdk/utils"
	"go.viam.com/test"
)

func TestFormatValid(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    Format
		ok   bool
	}{
		{"48k mono", Format{48000, 1}, true},
		{"44.1k stereo", Format{44100, 2}, true},
		{"zero", Format{}, false},
		{"rate too low", Format{4000, 1}, false},
		{"rate too high", Format{200000, 1}, false},
		{"too many channels", Format{48000, 3}, false},
		{"zero channels", Format{48000, 0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.f.Valid()
			if tc.ok {
				test.That(t, err, test.ShouldBeNil)
				return
			}
			test.That(t, err, test.ShouldNotBeNil)
		})
	}
}

func TestDurationForBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		f     Format
		bytes int
		want  time.Duration
	}{
		{"20ms at 48k mono", Format{48000, 1}, 1920, 20 * time.Millisecond},
		{"20ms at 48k stereo", Format{48000, 2}, 3840, 20 * time.Millisecond},
		// A degenerate format must not divide by zero.
		{"zero format", Format{}, 1920, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test.That(t, tc.f.DurationForBytes(tc.bytes), test.ShouldEqual, tc.want)
		})
	}
}

func TestBytesPerFrame(t *testing.T) {
	test.That(t, Format{48000, 1}.BytesPerFrame(), test.ShouldEqual, 2)
	test.That(t, Format{48000, 2}.BytesPerFrame(), test.ShouldEqual, 4)
}

// TestAudioInfoNeverNil guards the RDK data collector, which dereferences the
// first chunk's AudioInfo without a nil check.
func TestAudioInfoNeverNil(t *testing.T) {
	info := Format{48000, 1}.AudioInfo(rutils.CodecPCM16)
	test.That(t, info, test.ShouldNotBeNil)
	test.That(t, *info, test.ShouldResemble, rutils.AudioInfo{
		Codec: rutils.CodecPCM16, SampleRateHz: 48000, NumChannels: 1,
	})
}

func TestFromAudioInfo(t *testing.T) {
	t.Run("nil is an error, not a panic", func(t *testing.T) {
		_, err := FromAudioInfo(nil)
		test.That(t, err, test.ShouldWrap, ErrNoAudioInfo)
	})

	t.Run("round trips", func(t *testing.T) {
		want := Format{48000, 1}
		got, err := FromAudioInfo(want.AudioInfo(rutils.CodecPCM16))
		test.That(t, err, test.ShouldBeNil)
		test.That(t, got, test.ShouldResemble, want)
	})

	t.Run("rejects unsupported codecs", func(t *testing.T) {
		for _, codec := range []string{rutils.CodecOpus, rutils.CodecMP3, rutils.CodecPCM32, ""} {
			_, err := FromAudioInfo(&rutils.AudioInfo{
				Codec: codec, SampleRateHz: 48000, NumChannels: 1,
			})
			test.That(t, err, test.ShouldNotBeNil)
		}
	})

	t.Run("rejects an invalid format", func(t *testing.T) {
		_, err := FromAudioInfo(&rutils.AudioInfo{
			Codec: rutils.CodecPCM16, SampleRateHz: 0, NumChannels: 0,
		})
		test.That(t, err, test.ShouldNotBeNil)
	})
}

func TestPeakDBFS(t *testing.T) {
	for _, tc := range []struct {
		name string
		pcm  []byte
		want float64
	}{
		// A microphone denied by macOS TCC delivers a correctly-shaped stream of
		// pure zeros, which must stay distinguishable from quiet-but-live audio.
		{"digital silence", make([]byte, 1920), SilentDBFS},
		{"empty buffer", nil, SilentDBFS},
		{"full scale", []byte{0xFF, 0x7F}, 0},
		// -32768 negated naively wraps back to itself.
		{"negative full scale", []byte{0x00, 0x80}, 0},
		{"half scale", []byte{0x00, 0x40}, -6.02},
		{"peak across the buffer", []byte{0x01, 0x00, 0xFF, 0x7F}, 0},
		{"trailing odd byte ignored", []byte{0xFF, 0x7F, 0x00}, 0},
		{"a lone byte", []byte{0x42}, SilentDBFS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := PeakDBFS(tc.pcm)
			test.That(t, math.IsNaN(got) || math.IsInf(got, 0), test.ShouldBeFalse)
			test.That(t, got, test.ShouldAlmostEqual, tc.want, 0.05)
		})
	}
}
