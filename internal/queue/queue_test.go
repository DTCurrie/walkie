package queue

import (
	"sync"
	"testing"

	"go.viam.com/test"
)

func TestOfferDropsOldest(t *testing.T) {
	ch := make(chan byte, 2)

	test.That(t, Offer(ch, byte(1)), test.ShouldBeFalse)
	test.That(t, Offer(ch, byte(2)), test.ShouldBeFalse)
	test.That(t, Offer(ch, byte(3)), test.ShouldBeTrue)

	// The oldest item is the one that went away.
	test.That(t, []byte{<-ch, <-ch}, test.ShouldResemble, []byte{2, 3})
	test.That(t, ch, test.ShouldHaveLength, 0)
}

// TestOfferNeverBlocks: a queue nobody drains must still accept writes forever,
// or one dead subscriber takes down every talker on its channel.
func TestOfferNeverBlocks(t *testing.T) {
	ch := make(chan int, 4)
	for i := range 10_000 {
		Offer(ch, i)
	}
	test.That(t, ch, test.ShouldHaveLength, 4)
}

// TestConcurrentOfferIsSafe covers the bus's arrangement, where a talker's
// fan-out and the keepalive ticker write to one subscriber queue at once. Which
// items survive is deliberately unspecified; the bound is not.
func TestConcurrentOfferIsSafe(t *testing.T) {
	ch := make(chan int, 8)

	var wg sync.WaitGroup
	for p := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 5_000 {
				Offer(ch, p*10_000+i)
			}
		}()
	}

	// Not part of wg: it only stops once the producers have, so waiting on it
	// alongside them would deadlock.
	done := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)
	<-consumerDone

	test.That(t, len(ch), test.ShouldBeLessThanOrEqualTo, 8)
}
