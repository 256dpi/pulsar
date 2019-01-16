package pulsar

import (
	"sync/atomic"
	"testing"
)

func BenchmarkProducer(b *testing.B) {
	b.ReportAllocs()

	producer, err := CreateProducer(ProducerConfig{
		Topic: "public/test/bench1",
		ErrorCallback: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	msg := ProducerMessage{
		Payload: []byte("bench1"),
	}

	lastSendAck := make(chan struct{})

	var counter int64

	for i := 0; i < b.N; i++ {
		err = producer.Send(msg, func(_ ProducerMessage, err error) {
			if err != nil {
				panic(err)
			}

			if int(atomic.AddInt64(&counter, 1)) == b.N {
				close(lastSendAck)
			}
		})
		if err != nil {
			panic(err)
		}
	}

	safeWait(lastSendAck)

	err = producer.Close()
	if err != nil {
		panic(err)
	}
}
