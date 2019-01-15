package main

import (
	"time"

	"github.com/256dpi/pulsar"
)

const consumerInflightMessages = 1000

func consumer() {
	// prepare channel
	queue := make(chan pulsar.ConsumerMessage, consumerInflightMessages)

	// create consumer
	consumer, err := pulsar.CreateConsumer(pulsar.ConsumerConfig{
		Name:             "example",
		Topic:            "example",
		Subscription:     "example",
		SubscriptionType: pulsar.Exclusive,
		InflightMessages: consumerInflightMessages,
		MessageCallback: func(msg pulsar.ConsumerMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	for {
		// get next message
		msg := <-queue

		// increment
		recv++

		// parse time
		t, _ := time.Parse(time.RFC3339Nano, string(msg.Payload))

		// save diff
		diff = append(diff, float64(time.Since(t))/float64(time.Millisecond))

		// ack message
		err = consumer.AckCumulative(msg.ID)
		if err != nil {
			panic(err)
		}
	}
}
