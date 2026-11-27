package stream

import (
	"testing"
	"time"
)

// TestSlidingWindowBufferReleasesStorageAfterBurst checks the backing array
// follows the live window down after a burst, rather than keeping its
// high-water mark for the rest of the stream.
func TestSlidingWindowBufferReleasesStorageAfterBurst(t *testing.T) {
	t.Parallel()

	b, err := NewSlidingWindowBuffer(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 100_000; i++ { // a burst: 100k events within one second
		b.Add(at.Add(time.Duration(i)*time.Microsecond), "")
	}
	burstCap := cap(b.entries)
	// Then a quiet hour at one event per second.
	for i := 0; i < 3600; i++ {
		at = at.Add(time.Second)
		b.Add(at, "")
	}
	if got := cap(b.entries); got >= burstCap/4 || got > 4*61+4096 {
		t.Fatalf("cap(entries) after burst = %d (burst high-water %d); storage was not released", got, burstCap)
	}
	if b.Len() != 60 {
		t.Fatalf("Len() = %d, want 60", b.Len())
	}
}
