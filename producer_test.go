package pulsar

import (
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
