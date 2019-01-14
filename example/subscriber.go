package main

import (
	"time"

	"github.com/256dpi/pulsar"
	"github.com/256dpi/pulsar/frame"
)

func subscriber() {
	// lookup connection
	config, _, err := pulsar.Lookup("", "example")
	if err != nil {
		panic(err)
	}

	/// create client
	client, err := pulsar.Connect(config)
	if err != nil {
		panic(err)
	}

	// prepare counter
	counter := 0

	// create consumer
	var cid uint64
	done := make(chan struct{})
	err = client.CreateConsumer("", "example", "example", frame.Exclusive, false, func(id uint64, err error) {
		if err != nil {
			panic(err)
		}

		cid = id
		close(done)
	}, func(msg *frame.Message, err error) {
		// ack message
		err = client.Ack(cid, frame.Cumulative, msg.MessageID)
		if err != nil {
			panic(err)
		}

		// counter
		counter++

		// check counter
		if counter == 1000 {
			// request messages
			err = client.Flow(cid, 1000)
			if err != nil {
				panic(err)
			}

			counter = 0
		}

		// increment
		recv++

		// parse time
		t, _ := time.Parse(time.RFC3339Nano, string(msg.Payload))

		// save diff
		diff = append(diff, float64(time.Since(t)) / float64(time.Millisecond))
	})
	if err != nil {
		panic(err)
	}

	// request messages
	err = client.Flow(cid, 2000)
	if err != nil {
		panic(err)
	}

	select {}
}
