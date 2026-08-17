// Package audiofmt describes PCM audio formats on the wire and the math needed
// to reason about them. Everything here is signed 16-bit little-endian
// interleaved PCM, i.e. rutils.CodecPCM16.
package audiofmt

import (
	"errors"
	"fmt"
	"math"
	"time"

	rutils "go.viam.com/rdk/utils"
)

// BytesPerSample is the width of one sample of pcm16 audio.
const BytesPerSample = 2

// ErrNoAudioInfo is returned when a chunk arrives without the AudioInfo needed
// to interpret it. The RDK permits a nil AudioInfo on the wire, so this is a
// case to count rather than a case to panic on.
var ErrNoAudioInfo = errors.New("chunk has no audio_info")

// Format is the subset of rutils.AudioInfo that actually affects decoding.
// The codec is deliberately absent: this package only handles pcm16, and
// SupportedCodec guards the boundary.
type Format struct {
	SampleRateHz int
	NumChannels  int
}

// SupportedCodec reports whether a codec string is one this module can carry.
// Only pcm16 is supported; opus is a possible future addition, negotiated
// through the codec argument to GetAudio.
func SupportedCodec(codec string) bool {
	return codec == rutils.CodecPCM16
}

// String renders a format for logs and error messages, e.g. "48000Hz/1ch".
func (f Format) String() string {
	return fmt.Sprintf("%dHz/%dch", f.SampleRateHz, f.NumChannels)
}

// Valid reports whether the format is one we can actually play.
func (f Format) Valid() error {
	if f.SampleRateHz < 8000 || f.SampleRateHz > 192000 {
		return fmt.Errorf("sample_rate must be 8000..192000, got %d", f.SampleRateHz)
	}
	if f.NumChannels < 1 || f.NumChannels > 2 {
		return fmt.Errorf("num_channels must be 1 or 2, got %d", f.NumChannels)
	}
	return nil
}

// BytesPerFrame is the size of one interleaved frame across all channels.
func (f Format) BytesPerFrame() int {
	return f.NumChannels * BytesPerSample
}

// DurationForBytes reports how long a buffer of PCM will take to play.
// It returns 0 for a degenerate format rather than dividing by zero.
func (f Format) DurationForBytes(n int) time.Duration {
	if f.SampleRateHz <= 0 || f.NumChannels <= 0 {
		return 0
	}
	frames := n / f.BytesPerFrame()
	return time.Duration(frames) * time.Second / time.Duration(f.SampleRateHz)
}

// AudioInfo builds the RDK wire descriptor for this format. Never nil: the RDK
// data collector dereferences the first chunk's AudioInfo without a nil check
// (components/audioin/collectors.go).
func (f Format) AudioInfo(codec string) *rutils.AudioInfo {
	return &rutils.AudioInfo{
		Codec:        codec,
		SampleRateHz: int32(f.SampleRateHz),
		NumChannels:  int32(f.NumChannels),
	}
}

// FromAudioInfo reads a Format off an incoming chunk, rejecting anything this
// module cannot carry. A nil info is ErrNoAudioInfo, not a panic.
func FromAudioInfo(info *rutils.AudioInfo) (Format, error) {
	if info == nil {
		return Format{}, ErrNoAudioInfo
	}
	if !SupportedCodec(info.Codec) {
		return Format{}, fmt.Errorf("unsupported codec %q, only %q is supported", info.Codec, rutils.CodecPCM16)
	}
	f := Format{SampleRateHz: int(info.SampleRateHz), NumChannels: int(info.NumChannels)}
	if err := f.Valid(); err != nil {
		return Format{}, err
	}
	return f, nil
}

// SilentDBFS is the level reported for a buffer of pure digital silence.
// Real -inf is not representable in JSON, so this stands in for it and is far
// below anything a microphone produces.
const SilentDBFS = -120.0

// PeakDBFS returns the peak level of an interleaved pcm16 buffer in dBFS, where
// 0 is full scale, or SilentDBFS for an empty or all-zero buffer -- the
// signature of a microphone denied under macOS TCC.
func PeakDBFS(pcm []byte) float64 {
	peak := 0
	// Trailing odd byte, if any, cannot form a sample and is ignored.
	for i := 0; i+1 < len(pcm); i += 2 {
		// Little-endian signed 16-bit.
		v := int(int16(uint16(pcm[i]) | uint16(pcm[i+1])<<8))
		if v < 0 {
			// Negate via -(v+1) so math.MinInt16 does not overflow back to itself.
			v = -(v + 1)
		}
		if v > peak {
			peak = v
		}
	}
	if peak == 0 {
		return SilentDBFS
	}
	return 20 * math.Log10(float64(peak)/32767.0)
}
