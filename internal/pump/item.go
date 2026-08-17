package pump

import "walkie/internal/audiofmt"

// item is one chunk of PCM waiting to be played, tagged with its captured
// format. PlayStreamInit fixes the AudioInfo for a stream's life, so the sender
// needs this to notice a change and reopen.
type item struct {
	data   []byte
	format audiofmt.Format
}
