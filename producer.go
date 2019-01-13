package pulsar

import (
	"fmt"

	"github.com/256dpi/pulsar/frame"
)

type ProducerConfig struct {
	ClientConfig

	Name  string
	Topic string
}

type Producer struct {
	client *Client
}

func (c *Client) CreateProducer(config ProducerConfig) (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	c.producers++
	c.requests++

	// create producer frame
	producer := &frame.Producer{
		ID:        c.producers,
		Name:      config.Name,
		Topic:     config.Topic,
		RequestID: c.requests,
	}

	// send frame
	err := c.conn.Send(producer)
	if err != nil {
		return nil, err
	}

	// await response
	in, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}

	// check for error frame
	if _error, ok := in.(*frame.Error); ok {
		return nil, fmt.Errorf("error receied: %s, %s", _error.Error, _error.Message)
	}

	// get producer success frame
	producerSuccess, ok := in.(*frame.ProducerSuccess)
	if !ok {
		return nil, fmt.Errorf("expected to receive a connected frame")
	}

	// check request id
	if producerSuccess.RequestID != c.requests {
		return nil, fmt.Errorf("not matching request ids")
	}

	// check name
	if producerSuccess.Name != config.Name {
		return nil, fmt.Errorf("not matching producer names")
	}

	// create producer
	p := &Producer{
		client: c,
	}

	return p, nil
}
