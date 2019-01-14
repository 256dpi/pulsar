package pulsar

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	producer, err := CreateProducer(ProducerConfig{
		Topic: "test",
	})
	assert.NoError(t, err)

	msg := ProducerMessage{
		Payload: []byte("hello"),
	}

	done := make(chan struct{})
	err = producer.Send(msg, func(cbMsg ProducerMessage, cbErr error) {
		assert.NoError(t, cbErr)
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
		Topic: "test",
	})
	if err != nil {
		panic(err)
	}

	msg := ProducerMessage{
		Payload: []byte("hello"),
	}

	done := make(chan struct{})

	var counter int64

	for i := 0; i < b.N; i++ {
		err = producer.Send(msg, func(cbMsg ProducerMessage, cbErr error) {
			if cbErr != nil {
				panic(cbErr)
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
