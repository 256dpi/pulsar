package pulsar

import (
	"errors"
	"fmt"
	"sync"

	"github.com/256dpi/pulsar/frame"
)

var ErrRedirect = errors.New("redirect")

type ClientConfig struct {
	// The Pulsar connection URL. Will default to "pulsar://localhost:6650".
	URL string

	ProxyURL string

	// The client version sent to the broker.
	Version string
}

type LookupResponse struct {
	URL           string
	Authoritative bool
	Proxy         bool
}

type Client struct {
	config ClientConfig
	conn   Conn

	producers uint64
	consumers uint64
	requests  uint64

	mutex sync.Mutex
}

func Connect(config ClientConfig) (*Client, error) {
	// create connection
	conn, err := Dial(config.URL)
	if err != nil {
		return nil, err
	}

	// create connect frame
	connect := &frame.Connect{
		ClientVersion: config.Version,
		ProxyURL:      config.ProxyURL,
	}

	// send connect frame
	err = conn.Send(connect)
	if err != nil {
		return nil, err
	}

	// await response
	in, err := conn.Receive()
	if err != nil {
		return nil, err
	}

	// check for error frame
	if _error, ok := in.(*frame.Error); ok {
		return nil, fmt.Errorf("connection denied: %s, %s", _error.Error, _error.Message)
	}

	// get connected frame
	_, ok := in.(*frame.Connected)
	if !ok {
		return nil, fmt.Errorf("expected to receive a connected frame")
	}

	// create client
	client := &Client{
		config: config,
		conn:   conn,
	}

	return client, nil
}

func (c *Client) Lookup(topic string, authoritative bool) (*LookupResponse, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	c.requests++

	// create lookup frame
	lookup := &frame.Lookup{
		Topic:         topic,
		RequestID:     c.requests,
		Authoritative: authoritative,
	}

	// send lookup frame
	err := c.conn.Send(lookup)
	if err != nil {
		return nil, err
	}

	// await response
	in, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}

	// get lookup response frame
	lookupResponse, ok := in.(*frame.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("expected to receive a lookup response frame")
	}

	// check if failed
	if lookupResponse.Response == frame.LookupTypeFailed {
		return nil, fmt.Errorf("lookup failed: %s, %s", lookupResponse.Error, lookupResponse.Message)
	}

	// prepare response
	resp := &LookupResponse{
		URL:           lookupResponse.BrokerServiceURL,
		Authoritative: lookupResponse.Authoritative,
		Proxy:         lookupResponse.ProxyThroughServiceURL,
	}

	// check if needs redirect
	if lookupResponse.Response == frame.LookupTypeRedirect {
		return resp, ErrRedirect
	}

	return resp, nil
}

func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close connection
	err := c.conn.Close()
	if err != nil {
		return err
	}

	return nil
}
