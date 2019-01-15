package pulsar

import (
	"testing"
	"time"

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
		ErrorCallback: func(closed bool, cbErr error) {
			assert.NoError(t, cbErr)
			assert.False(t, closed)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test7", []byte("test7"))

	msg := <-queue

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestFailoverConsumer(t *testing.T) {
	t.Skip()
	return

	queue := make(chan ConsumerMessage, 1)

	consumer, err := CreateFailoverConsumer(ConsumerConfig{
		Name:             "test8",
		Topic:            "public/test/test8",
		Subscription:     "test8",
		InflightMessages: 1,
		MessageCallback: func(msg ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(closed bool, cbErr error) {
			assert.NoError(t, cbErr)
			assert.False(t, closed)
		},
	})
	assert.NoError(t, err)

	println("first done")

	time.Sleep(time.Second)

	sendMessage("public/test/test8", []byte("test8"))

	msg := <-queue

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
		ErrorCallback: func(closed bool, cbErr error) {
			assert.NoError(t, cbErr)
			assert.False(t, closed)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test9", []byte("test9"))

	msg := <-queue

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}
