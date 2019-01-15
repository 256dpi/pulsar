package pulsar

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	producer, err := CreateProducer(ProducerConfig{
		Topic: "public/test/test6",
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	assert.NotEmpty(t, producer.Name())

	msg := ProducerMessage{
		Payload: []byte("test6"),
	}

	done := make(chan struct{})
	err = producer.Send(msg, func(cbMsg ProducerMessage, err error) {
		assert.NoError(t, err)
		assert.Equal(t, msg, cbMsg)

		close(done)
	})
	assert.NoError(t, err)

	safeWait(done)

	err = producer.Close()
	assert.NoError(t, err)
}

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

	done := make(chan struct{})

	var counter int64

	for i := 0; i < b.N; i++ {
		err = producer.Send(msg, func(_ ProducerMessage, err error) {
			if err != nil {
				panic(err)
			}

			if int(atomic.AddInt64(&counter, 1)) == b.N {
				close(done)
			}
		})
		if err != nil {
			panic(err)
		}
	}

	safeWait(done)

	err = producer.Close()
	if err != nil {
		panic(err)
	}
}
