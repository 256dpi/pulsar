package pulsar

import (
	"fmt"

	"github.com/256dpi/pulsar/frame"
)

type ClientConfig struct {
	// The Pulsar connection URL. Will default to "pulsar://localhost:6650".
	URL string

	// The client version sent to the broker.
	Version string
}

type Client struct {
	config ClientConfig

	conn Conn
}

func NewClient(config ClientConfig) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) Connect() error {
	// create connection
	conn, err := Dial(c.config.URL)
	if err != nil {
		return err
	}

	// create connect cmd
	connect := &frame.Connect{
		ClientVersion: c.config.Version,
	}

	// send connect
	err = conn.Send(connect)
	if err != nil {
		return err
	}

	// await response
	in, err := conn.Receive()
	if err != nil {
		return err
	}

	// check for error frame
	if _error, ok := in.(*frame.Error); ok {
		return fmt.Errorf("connection denied: %s, %s", _error.Error, _error.Message)
	}

	// check if connected frame
	if _, ok := in.(*frame.Connected); !ok {
		return fmt.Errorf("expected to receive a connected frame")
	}

	// save conn
	c.conn = conn

	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
