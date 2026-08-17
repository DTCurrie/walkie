package bus

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The sentinel errors the bus returns. Each maps to exactly one gRPC code in
// ToStatus, and that mapping is the module's whole vocabulary for "why did that
// not work" as seen from a member machine.
var (
	// ErrBusy means another member holds the channel's floor. First talker wins.
	ErrBusy = errors.New(BusyMsg)
	// ErrRevoked means this transmission lost the floor while it was running --
	// the bus closed, an operator cleared the floor, or the watchdog decided the
	// talker had gone away.
	ErrRevoked = errors.New(RevokedMsg)
	// ErrUnknownChannel means the channel is not one the bus declares.
	ErrUnknownChannel = errors.New("no such channel")
	// ErrFormat means the transmission's format is not what the channel carries.
	ErrFormat = errors.New("wrong format for this channel")
	// ErrClosed means the bus has been shut down, which on a live machine means
	// it is mid-rebuild and will be back.
	ErrClosed = errors.New("bus is closed")
)

// BusyMsg and RevokedMsg are stable substrings, not prose. An error crosses
// three hops, each wrapping with %w: the gRPC code survives, structured details
// do not. Both halves must stay put.
const (
	BusyMsg    = "walkie: channel busy"
	RevokedMsg = "walkie: transmission revoked"
)

// BusyError adds the current holder to ErrBusy without breaking errors.Is.
func BusyError(holder string) error {
	return fmt.Errorf("%w: %q is talking", ErrBusy, holder)
}

// UnknownChannelError names the channel and what is on offer instead.
func UnknownChannelError(name string, known []string) error {
	return fmt.Errorf("%w: %q; this bus carries %s",
		ErrUnknownChannel, name, strings.Join(known, ", "))
}

// ToStatus maps a bus error onto the gRPC code a member sees.
// FailedPrecondition for ErrBusy is load-bearing: Unavailable is
// indistinguishable from a dead hub, ResourceExhausted invites retries.
func ToStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrBusy):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrRevoked):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrUnknownChannel):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrFormat):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrClosed):
		// Unavailable is right here and wrong for ErrBusy: a closed bus really is
		// a machine that will be back in a moment, which is what Unavailable means.
		return status.Error(codes.Unavailable, err.Error())
	default:
		return err
	}
}

// IsBusy reports whether err is a rejected transmission from a busy channel,
// after any wrapping and a round trip through gRPC. That flattens it to a
// string, so errors.Is alone is not enough.
func IsBusy(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrBusy) {
		return true
	}
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), BusyMsg)
}

// IsRevoked reports whether err is a transmission that lost the floor, subject
// to the same round-trip caveats as IsBusy.
func IsRevoked(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRevoked) {
		return true
	}
	return status.Code(err) == codes.Aborted && strings.Contains(err.Error(), RevokedMsg)
}
