package pulsar

import (
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
