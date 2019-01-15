package pulsar

import "time"

func ensureTopic(topic string) {
	_, _, err := Lookup(defaultAddr, topic)
	if err != nil {
		panic(err)
	}
}

func safeWait(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		panic("nothing received")
	}
}
