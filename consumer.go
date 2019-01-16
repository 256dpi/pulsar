package pulsar

import (
	"fmt"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"
)

// SubscriptionType defines the subscription type.
type SubscriptionType = frame.SubscriptionType

const (
	// Exclusive subscriptions are only allowed to be subscribed by one client.
	// Additional subscriptions will return an error.
	Exclusive = frame.Exclusive

	// Shared subscriptions allow messages to be distributed among the consumers.
	Shared = frame.Shared

	// Failover subscriptions allow additional consumers to take over when the
	// active consumer fail.s
	Failover = frame.Failover
)

// ConsumerConfig holds the configuration for a consumer.
type ConsumerConfig struct {
	// The service URL of the Pulsar broker.
	ServiceURL string

	// The topic to attach to.
	Topic string

	// The subscription name.
	Subscription string

	// The callback that is called with incoming messages.
	MessageCallback func(ConsumerMessage)

	// The callback that is called when the consumer has been closed by the
	// broker, the end of the topic has been reached or the underlying client
	// failed. The underlying client is only closed on network level errors.
	ErrorCallback func(error)

	/* Optional settings */

	// The consumer name.
	Name string

	// The subscription type.
	//
	// Default: Exclusive.
	SubscriptionType SubscriptionType

	// If set a newly created subscription will start from the earliest message
	// available instead of the latest.
	StartFromEarliestMessage bool

	// InflightMessages can be set to perform automatic flow control.
	InflightMessages int

	// The callback that is called when the state of the consumer has been
	// changed by the broker.
	StateCallback func(bool)

	// The timeout after the creation request triggers and error.
	//
	// Default: 10s.
	CreateTimeout time.Duration

	// The timeout after the close request triggers and error.
	//
	// Default: 10s.
	CloseTimeout time.Duration

	// The timeout after which writes to the underlying buffered writer are
	// flushed. This value should not be set to low as it might trigger
	// repeatedly launch goroutines that attempt fo flush the buffer.
	//
	// Default: 100ms.
	WriteTimeout time.Duration
}

// ConsumerMessage is a single message consumed by a consumer.
type ConsumerMessage struct {
	// The message id.
	ID MessageID

	// The message payload.
	Payload []byte
}

// Consumer allows messages to be consumed from a Pulsar topic.
type Consumer struct {
	config ConsumerConfig
	client *Client

	cid     uint64
	counter int

	mutex sync.Mutex
}

// CreateConsumer will setup and return a consumer.
func CreateConsumer(config ConsumerConfig) (*Consumer, error) {
	// set default create request timeout
	if config.CreateTimeout == 0 {
		config.CreateTimeout = 10 * time.Second
	}

	// set default close request timeout
	if config.CloseTimeout == 0 {
		config.CloseTimeout = 10 * time.Second
	}

	// set default empty state callback
	if config.StateCallback == nil {
		config.StateCallback = func(bool) {}
	}

	// perform lookup
	clientConfig, _, err := Lookup(config.ServiceURL, config.Topic)
	if err != nil {
		return nil, err
	}

	// set write timeout
	clientConfig.WriteTimeout = config.WriteTimeout

	// create client
	client, err := Connect(clientConfig)
	if err != nil {
		return nil, err
	}

	// prepare initial position
	initialPos := frame.Latest
	if config.StartFromEarliestMessage {
		initialPos = frame.Earliest
	}

	// prepare consumer
	consumer := &Consumer{
		config: config,
		client: client,
	}

	// create consumer
	res := make(chan error, 1)
	err = client.CreateConsumer(config.Name, config.Topic, config.Subscription, config.SubscriptionType, true, initialPos, nil, func(cid uint64, err error) {
		// set pid and sequence
		consumer.cid = cid

		// forward response
		select {
		case res <- err:
		default:
		}
	}, func(msg *frame.Message, active *bool, err error) {
		// handle error
		if err != nil {
			config.ErrorCallback(err)
			return
		}

		// handle active
		if active != nil {
			config.StateCallback(*active)
			return
		}

		// call message callback
		config.MessageCallback(ConsumerMessage{
			ID:      msg.MessageID,
			Payload: msg.Payload,
		})

		// perform flow control if enabled
		if config.InflightMessages > 0 {
			// increment counter
			consumer.counter++

			// check flow
			if consumer.counter > config.InflightMessages/2 {
				// request more messages
				err = client.Flow(consumer.cid, uint32(consumer.counter))
				if err != nil {
					config.ErrorCallback(err)
					return
				}

				// reset counter
				consumer.counter = 0
			}
		}
	})
	if err != nil {
		return nil, err
	}

	// wait for response
	select {
	case err = <-res:
		if err != nil {
			return nil, err
		}
	case <-time.After(config.CreateTimeout):
		return nil, ErrTimeout
	}

	// issue first flow if enabled
	if config.InflightMessages > 0 {
		err = client.Flow(consumer.cid, uint32(config.InflightMessages))
		if err != nil {
			return nil, err
		}
	}

	return consumer, nil
}

// Flow asks the broker to queue the specified amount of messages.
func (c *Consumer) Flow(messages int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check config
	if c.config.InflightMessages > 0 {
		return fmt.Errorf("automatic flow control enabled")
	}

	// send ack
	err := c.client.Flow(c.cid, uint32(messages))
	if err != nil {
		return err
	}

	return nil
}

// AckIndividual will ack the specified message.
func (c *Consumer) AckIndividual(mid frame.MessageID) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// send ack
	err := c.client.Ack(c.cid, frame.Individual, mid)
	if err != nil {
		return err
	}

	return nil
}

// AckCumulative will ack all previous and the specified message.
func (c *Consumer) AckCumulative(mid frame.MessageID) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check subscription type
	if c.config.SubscriptionType == frame.Shared {
		return fmt.Errorf("cumulative ack not supported for shared subscriptions")
	}

	// send ack
	err := c.client.Ack(c.cid, frame.Cumulative, mid)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the consumer. It is the callers responsibility to make sure
// messages have been acknowledged before calling Close.
func (c *Consumer) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// create producer
	res := make(chan error, 1)
	err := c.client.CloseConsumer(c.cid, func(err error) {
		// forward response
		select {
		case res <- err:
		default:
		}
	})
	if err != nil {
		return err
	}

	// wait for response
	select {
	case err = <-res:
		if err != nil {
			return err
		}
	case <-time.After(c.config.CloseTimeout):
		return ErrTimeout
	}

	// close client
	err = c.client.Close()
	if err != nil {
		return err
	}

	return nil
}
