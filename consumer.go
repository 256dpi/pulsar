package pulsar

import (
	"fmt"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"
)

// ConsumerConfig holds the configuration for a consumer.
type ConsumerConfig struct {
	// The service URL of the Pulsar broker.
	ServiceURL string

	// The consumer's name.
	Name string

	// The topic to attach to.
	Topic string

	// The subscription name.
	Subscription string

	// If set a newly created subscription will start from the earliest message
	// available.
	StartFromEarliestMessage bool

	// InflightMessages can be set to perform automatic flow control.
	InflightMessages int

	// The callback that is called with incoming messages.
	MessageCallback func(ConsumerMessage)

	// The callback that is called when the state of the consumer has been
	// changed by the broker.
	StateCallback func(bool)

	// The callback that is called when the consumer has been closed by the
	// broker, the end of the topic has been reached or the underlying client
	// failed.
	ErrorCallback func(error)

	// The timeout after the creation request triggers and error.
	CreateTimeout time.Duration

	// The timeout after the close request triggers and error.
	CloseTimeout time.Duration
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
	shared  bool
	counter int

	mutex sync.Mutex
}

// CreateSharedConsumer will setup and return a shared consumer.
func CreateSharedConsumer(config ConsumerConfig) (*Consumer, error) {
	return createGenericConsumer(config, frame.Shared)
}

// CreateFailoverConsumer will setup and return a failover consumer.
func CreateFailoverConsumer(config ConsumerConfig) (*Consumer, error) {
	return createGenericConsumer(config, frame.Failover)
}

// CreateExclusiveConsumer will setup and return an exclusive consumer.
func CreateExclusiveConsumer(config ConsumerConfig) (*Consumer, error) {
	return createGenericConsumer(config, frame.Exclusive)
}

func createGenericConsumer(config ConsumerConfig, typ frame.SubscriptionType) (*Consumer, error) {
	// set default create request timeout
	if config.CreateTimeout == 0 {
		config.CreateTimeout = DefaultTimeout
	}

	// set default close request timeout
	if config.CloseTimeout == 0 {
		config.CloseTimeout = DefaultTimeout
	}

	// perform lookup
	clientConfig, _, err := Lookup(config.ServiceURL, config.Topic)
	if err != nil {
		return nil, err
	}

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
		shared: typ == frame.Shared,
	}

	// create consumer
	res := make(chan error, 1)
	err = client.CreateConsumer(config.Name, config.Topic, config.Subscription, typ, true, initialPos, nil, func(cid uint64, err error) {
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
	if c.shared {
		return fmt.Errorf("cumulative ack not supported for shared subscriptions")
	}

	// send ack
	err := c.client.Ack(c.cid, frame.Cumulative, mid)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the producer and potentially cancel outstanding send requests.
func (c *Consumer) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// TODO: Wait for all messages to be consumed?

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
