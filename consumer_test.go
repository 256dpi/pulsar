package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSharedConsumer(t *testing.T) {
	queue := make(chan interface{}, 1)

	consumer, err := CreateConsumer(ConsumerConfig{
		Name:             "test7",
		Topic:            "public/test/test7",
		Subscription:     "test7",
		SubscriptionType: Shared,
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

	msg := safeReceive(queue).(ConsumerMessage)
	assert.Equal(t, []byte("test7"), msg.Payload)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestFailoverConsumer(t *testing.T) {
	queue := make(chan interface{}, 1)

	consumer, err := CreateConsumer(ConsumerConfig{
		Name:             "test8",
		Topic:            "public/test/test8",
		Subscription:     "test8",
		SubscriptionType: Failover,
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

	msg := safeReceive(queue).(ConsumerMessage)
	assert.Equal(t, []byte("test8"), msg.Payload)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestExclusiveConsumer(t *testing.T) {
	queue := make(chan interface{}, 1)

	consumer, err := CreateConsumer(ConsumerConfig{
		Name:             "test9",
		Topic:            "public/test/test9",
		Subscription:     "test9",
		SubscriptionType: Exclusive,
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

	msg := safeReceive(queue).(ConsumerMessage)
	assert.Equal(t, []byte("test9"), msg.Payload)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestManualFlowControl(t *testing.T) {
	queue := make(chan interface{}, 1)

	consumer, err := CreateConsumer(ConsumerConfig{
		Name:             "test10",
		Topic:            "public/test/test10",
		Subscription:     "test10",
		SubscriptionType: Shared,
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

	msg := safeReceive(queue).(ConsumerMessage)
	assert.Equal(t, []byte("test10"), msg.Payload)

	err = consumer.AckIndividual(msg.ID)
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)
}
