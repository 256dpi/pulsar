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

	b.ResetTimer()

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

func BenchmarkConsumer(b *testing.B) {
	b.ReportAllocs()

	queue := make(chan ConsumerMessage, 100)

	consumer, err := CreateConsumer(ConsumerConfig{
		Topic:                    "public/test/bench2",
		Subscription:             "bench2",
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			panic(err)
		},
	})

	producer, err := CreateProducer(ProducerConfig{
		Topic: "public/test/bench2",
		ErrorCallback: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	msg := ProducerMessage{
		Payload: []byte("bench2"),
	}

	for i:=0; i<b.N; i++ {
		err = producer.Send(msg, func(_ ProducerMessage, err error) {
			if err != nil {
				panic(err)
			}
		})
		if err != nil {
			panic(err)
		}
	}

	err = consumer.Flow(b.N)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	counter := 0

	for {
		msg := <-queue

		err = consumer.AckCumulative(msg.ID)
		if err != nil {
			panic(err)
		}

		counter++
		if counter == b.N {
			break
		}
	}

	err = consumer.Close()
	if err != nil {
		panic(err)
	}
}
