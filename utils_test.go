package pulsar

import "time"

func safeWait(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		panic("nothing received")
	}
}
