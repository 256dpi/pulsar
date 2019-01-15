package pulsar

import (
	"fmt"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"
)

// ReaderConfig holds the configuration for a reader.
type ReaderConfig struct {
	// The service URL of the Pulsar broker.
	ServiceURL string

	// The reader's name.
	Name string

	// The topic to attach to.
	Topic string

	// The subscription name.
	Subscription string

	// If set the reader will start from the earliest message available.
	StartFromEarliestMessage bool

	// If set the reader will start from the provided message.
	StartMessageID *MessageID

	// InflightMessages can be set to perform automatic flow control.
	InflightMessages int

	// The callback that is called with incoming messages.
	MessageCallback func(ReaderMessage)

	// The callback that is called when the reader has been closed by the
	// broker, the end of the topic has been reached or the underlying client
	// failed.
	ErrorCallback func(error)

	// The timeout after the creation request triggers and error.
	CreateTimeout time.Duration

	// The timeout after the close request triggers and error.
	CloseTimeout time.Duration
}

// ReaderMessage is a single message consumed by a reader.
type ReaderMessage struct {
	// The message id.
	ID MessageID

	// The message payload.
	Payload []byte
}

// Reader allows messages to be consumed from a Pulsar topic.
type Reader struct {
	config ReaderConfig
	client *Client

	cid     uint64
	counter int

	mutex sync.Mutex
}

// CreateReader will setup and return a shared reader.
func CreateReader(config ReaderConfig) (*Reader, error) {
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

	// prepare reader
	reader := &Reader{
		config: config,
		client: client,
	}

	// create reader
	res := make(chan error, 1)
	err = client.CreateConsumer(config.Name, config.Topic, config.Subscription, frame.Exclusive, false, initialPos, config.StartMessageID, func(cid uint64, err error) {
		// set pid and sequence
		reader.cid = cid

		// forward response
		select {
		case res <- err:
		default:
		}
	}, func(msg *frame.Message, _ *bool, err error) {
		// handle error
		if err != nil {
			config.ErrorCallback(err)
			return
		}

		// call message callback
		config.MessageCallback(ReaderMessage{
			ID:      msg.MessageID,
			Payload: msg.Payload,
		})

		// perform flow control if enabled
		if config.InflightMessages > 0 {
			// increment counter
			reader.counter++

			// check flow
			if reader.counter > config.InflightMessages/2 {
				// request more messages
				err = client.Flow(reader.cid, uint32(reader.counter))
				if err != nil {
					config.ErrorCallback(err)
					return
				}

				// reset counter
				reader.counter = 0
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
		err = client.Flow(reader.cid, uint32(config.InflightMessages))
		if err != nil {
			return nil, err
		}
	}

	return reader, nil
}

// Flow asks the broker to queue the specified amount of messages.
func (c *Reader) Flow(messages int) error {
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

// Close will close the reader.
func (c *Reader) Close() error {
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
