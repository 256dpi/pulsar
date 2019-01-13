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
		RID:           c.requests,
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

func (c *Client) CreateProducer(name, topic string) (uint64, int64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	c.requests++
	c.producers++

	// create producer frame
	producer := &frame.Producer{
		RID:   c.requests,
		ID:    c.producers,
		Name:  name,
		Topic: topic,
	}

	// send frame
	err := c.conn.Send(producer)
	if err != nil {
		return 0, 0, err
	}

	// await response
	in, err := c.conn.Receive()
	if err != nil {
		return 0, 0, err
	}

	// check for error frame
	if _error, ok := in.(*frame.Error); ok {
		return 0, 0, fmt.Errorf("error receied: %s, %s", _error.Error, _error.Message)
	}

	// get producer success frame
	producerSuccess, ok := in.(*frame.ProducerSuccess)
	if !ok {
		return 0, 0, fmt.Errorf("expected to receive a connected frame")
	}

	// check request id
	if producerSuccess.RID != c.requests {
		return 0, 0, fmt.Errorf("not matching request ids")
	}

	// check name
	if producerSuccess.Name != name {
		return 0, 0, fmt.Errorf("not matching producer names")
	}

	return c.producers, producerSuccess.LastSequence, nil
}

func (c *Client) CloseProducer(id uint64) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	c.requests++

	// create producer frame
	producer := &frame.CloseProducer{
		RID: c.requests,
		ID:  id,
	}

	// send frame
	err := c.conn.Send(producer)
	if err != nil {
		return err
	}

	// await response
	in, err := c.conn.Receive()
	if err != nil {
		return err
	}

	// check for error frame
	if _error, ok := in.(*frame.Error); ok {
		return fmt.Errorf("error receied: %s, %s", _error.Error, _error.Message)
	}

	// get success frame
	success, ok := in.(*frame.Success)
	if !ok {
		return fmt.Errorf("expected to receive a sucess frame")
	}

	// check request id
	if success.RID != c.requests {
		return fmt.Errorf("not matching request ids")
	}

	return nil
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
