package pulsar

import (
	"sync"
	"time"
)

// ProducerConfig holds the configuration for a producer.
type ProducerConfig struct {
	// The service URL of the Pulsar broker.
	ServiceURL string

	// The topic to attach to.
	Topic string

	// The producer's name.
	//
	// A unique name will be generated by the broker if left empty.
	Name string

	// The callback that is called when the underlying client fails or the
	// broker requested the producer to close and reconnect.
	Callback func(closed bool, err error)

	// The timeout after the creation request triggers and error.
	CreateTimeout time.Duration

	// The timeout after the close request triggers and error.
	CloseTimeout time.Duration
}

// ProducerMessage is a single message produced by a producer.
type ProducerMessage struct {
	// The payload of the message.
	Payload []byte
}

// Producer allows messages to be produced on a Pulsar topic.
type Producer struct {
	config ProducerConfig
	client *Client

	pid  uint64
	name string
	seq  uint64

	mutex sync.Mutex
}

// CreateProducer will setup and return a producer.
func CreateProducer(config ProducerConfig) (*Producer, error) {
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

	// prepare producer
	producer := &Producer{
		config: config,
		client: client,
	}

	// create producer
	res := make(chan error, 1)
	err = client.CreateProducer(config.Name, config.Topic, func(pid uint64, name string, lastSeq int64, cbErr error) {
		// set pid and sequence
		producer.pid = pid
		producer.name = name
		if lastSeq > 0 {
			producer.seq = uint64(lastSeq)
		}

		// forward response
		select {
		case res <- cbErr:
		default:
		}
	}, config.Callback)
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

	return producer, nil
}

// Send will send the specified message and call the provided callback with the
// result.
func (p *Producer) Send(msg ProducerMessage, cb func(ProducerMessage, error)) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// increment sequence
	p.seq++

	// send message
	err := p.client.Send(p.pid, p.seq, msg.Payload, func(cbErr error) {
		if cb != nil {
			cb(msg, cbErr)
		}
	})
	if err != nil {
		return err
	}

	return nil
}

// Name will return the name of the producer.
func (p *Producer) Name() string {
	return p.name
}

// Close will close the producer and potentially cancel outstanding send requests.
func (p *Producer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// TODO: Wait for all outstanding send requests to complete?

	// create producer
	res := make(chan error, 1)
	err := p.client.CloseProducer(p.pid, func(cbErr error) {
		// forward response
		select {
		case res <- cbErr:
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
	case <-time.After(p.config.CloseTimeout):
		return ErrTimeout
	}

	// close client
	err = p.client.Close()
	if err != nil {
		return err
	}

	return nil
}
