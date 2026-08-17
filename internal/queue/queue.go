// Package queue holds the one queueing discipline this module uses everywhere,
// for both a sink that cannot keep up and a subscriber that has stopped reading:
// bounded, drop-oldest, never blocking.
package queue

// Offer queues an item, discarding the oldest if full. It never blocks, reports
// whether anything was discarded, and bounds live-audio latency. Concurrent
// producers are safe but may cost a discard.
func Offer[T any](ch chan T, it T) (dropped bool) {
	select {
	case ch <- it:
		return false
	default:
	}

	// Full. Discard the oldest to make room for the newest.
	select {
	case <-ch:
		dropped = true
	default:
		// The consumer emptied it between our two operations; nothing to discard.
	}

	select {
	case ch <- it:
	default:
		// The consumer refilled it just as fast. Discarding the new item still
		// bounds latency, which is the whole point.
		dropped = true
	}
	return dropped
}
