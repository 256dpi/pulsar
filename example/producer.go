package main

import (
	"time"

	"github.com/256dpi/pulsar"
)

const producerInflightMessages = 1000
const producerSendInterval = 20 * time.Microsecond

func producer() {
	// crete producer
	producer, err := pulsar.CreateProducer(pulsar.ProducerConfig{
		Topic: "example",
		ErrorCallback: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	// create and fill bucket
	bucket := make(chan int, producerInflightMessages)
	for i := 0; i < producerInflightMessages; i++ {
		bucket <- i
	}

	// send messages
	for {
		// get token
		token := <-bucket

		// prepare message
		message := pulsar.ProducerMessage{
			Payload: []byte(time.Now().Format(time.RFC3339Nano)),
		}

		// send message
		err = producer.Send(message, func(_ pulsar.ProducerMessage, err error) {
			if err != nil {
				panic(err)
			}

			// put back token
			bucket <- token
		})
		if err != nil {
			panic(err)
		}

		// increment
		send++

		// limit send rate
		time.Sleep(producerSendInterval)
	}
}
