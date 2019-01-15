package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSharedConsumer(t *testing.T) {
	queue := make(chan ConsumerMessage, 1)

	consumer, err := CreateSharedConsumer(ConsumerConfig{
		Name:             "test7",
		Topic:            "public/test/test7",
		Subscription:     "test7",
		InflightMessages: 1,
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test7", []byte("test7"))

	msg := safeReceive(queue)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestFailoverConsumer(t *testing.T) {
	queue := make(chan ConsumerMessage, 1)

	consumer, err := CreateFailoverConsumer(ConsumerConfig{
		Name:             "test8",
		Topic:            "public/test/test8",
		Subscription:     "test8",
		InflightMessages: 1,
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		StateCallback: func(active bool) {
			assert.True(t, active)
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test8", []byte("test8"))

	msg := safeReceive(queue)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestExclusiveConsumer(t *testing.T) {
	queue := make(chan ConsumerMessage, 1)

	consumer, err := CreateExclusiveConsumer(ConsumerConfig{
		Name:             "test9",
		Topic:            "public/test/test9",
		Subscription:     "test9",
		InflightMessages: 1,
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test9", []byte("test9"))

	msg := safeReceive(queue)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestManualFlowControl(t *testing.T) {
	queue := make(chan ConsumerMessage, 1)

	consumer, err := CreateSharedConsumer(ConsumerConfig{
		Name:         "test10",
		Topic:        "public/test/test10",
		Subscription: "test10",
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test10", []byte("test10"))

	assert.Len(t, queue, 0)

	err = consumer.Flow(1)
	assert.NoError(t, err)

	msg := safeReceive(queue)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}
