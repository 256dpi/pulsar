package pulsar

import "time"

func ensureTopic(topic string) {
	_, _, err := Lookup(defaultAddr, topic)
	if err != nil {
		panic(err)
	}
}

func sendMessage(topic string, payload []byte) {
	producer, err := CreateProducer(ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		panic(err)
	}

	msg := ProducerMessage{
		Payload: payload,
	}

	done := make(chan struct{})
	err = producer.Send(msg, func(_ ProducerMessage, cbErr error) {
		if cbErr != nil {
			panic(cbErr)
		}

		close(done)
	})
	if err != nil {
		panic(err)
	}

	safeWait(done)

	err = producer.Close()
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
