package pulsar

import (
	"fmt"
	"sync"

	"github.com/256dpi/pulsar/frame"
)

// Client is the low level client that exchanges frames with the pulsar broker.
type Client struct {
	conn *Conn

	producers uint64
	consumers uint64
	requests  uint64

	requestCallbacks  map[uint64]func(frame.Frame, error)
	producerCallbacks map[uint64]func(frame.Frame, error)
	sendCallbacks     map[string]func(frame.Frame, error)
	consumerCallbacks map[uint64]func(frame.Frame, error)

	mutex sync.Mutex
}

// Connect will connect to the provided broker and return a client.
func Connect(url, proxyURL, version string) (*Client, error) {
	// create connection
	conn, err := Dial(url)
	if err != nil {
		return nil, err
	}

	// create connect frame
	connect := &frame.Connect{
		ClientVersion:  version,
		ProxyBrokerURL: proxyURL,
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
		return nil, _error
	}

	// get connected frame
	_, ok := in.(*frame.Connected)
	if !ok {
		return nil, fmt.Errorf("expected to receive a connected frame")
	}

	// create client
	client := NewClient(conn)

	return client, nil
}

// NewClient creates a client using the specified connection.
func NewClient(conn *Conn) *Client {
	// create client
	client := &Client{
		conn:              conn,
		requestCallbacks:  make(map[uint64]func(frame.Frame, error)),
		producerCallbacks: make(map[uint64]func(frame.Frame, error)),
		sendCallbacks:     make(map[string]func(frame.Frame, error)),
		consumerCallbacks: make(map[uint64]func(frame.Frame, error)),
	}

	// run receiver
	go client.receiver()

	return client
}

// Lookup will send a lookup request and call the provided callback with the
// response.
func (c *Client) Lookup(topic string, authoritative bool, rcb func(*frame.LookupResponse, error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	rid := c.requests
	c.requests++

	// create lookup frame
	lookup := &frame.Lookup{
		Topic:         topic,
		RID:           rid,
		Authoritative: authoritative,
	}

	// store callback
	if rcb != nil {
		c.requestCallbacks[rid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				rcb(nil, err)
				return
			}

			// get lookup response frame
			lookupResponse, ok := res.(*frame.LookupResponse)
			if !ok {
				rcb(nil, fmt.Errorf("expected to receive a lookup response frame"))
				return
			}

			// check if failed
			if lookupResponse.ResponseType == frame.Failed {
				rcb(nil, lookupResponse)
				return
			}

			// call callback
			rcb(lookupResponse, nil)
		}
	}

	// send lookup frame
	err := c.send(lookup)
	if err != nil {
		return err
	}

	return nil
}

// CreateProducer will send a create producer request and call the provided callback
// with the response.
func (c *Client) CreateProducer(name, topic string, rcb func(uint64, int64, error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	rid := c.requests
	pid := c.producers
	c.requests++
	c.producers++

	// create producer frame
	producer := &frame.Producer{
		RID:   rid,
		PID:   pid,
		Name:  name,
		Topic: topic,
	}

	// store callback
	if rcb != nil {
		c.requestCallbacks[rid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				rcb(0, 0, err)
				return
			}

			// check for error frame
			if _error, ok := res.(*frame.Error); ok {
				rcb(0, 0, _error)
				return
			}

			// get producer success frame
			producerSuccess, ok := res.(*frame.ProducerSuccess)
			if !ok {
				rcb(0, 0, fmt.Errorf("expected to receive a connected frame"))
				return
			}

			// check request id
			if producerSuccess.RID != rid {
				rcb(0, 0, fmt.Errorf("not matching request ids"))
				return
			}

			// check name
			if producerSuccess.Name != name {
				rcb(0, 0, fmt.Errorf("not matching producer names"))
				return
			}

			// call callback
			rcb(pid, producerSuccess.LastSequence, nil)
		}
	}

	// send frame
	err := c.send(producer)
	if err != nil {
		return err
	}

	return nil
}

// Send will perform send request and call the provided callback with the
// response.
func (c *Client) Send(pid, seq uint64, msg []byte, scb func(error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// create send frame
	producer := &frame.Send{
		PID:      pid,
		Sequence: seq,
		Message:  msg,
	}

	// store send callback
	if scb != nil {
		c.sendCallbacks[sendKey(pid, seq)] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				scb(err)
				return
			}

			// check for error frame
			if _error, ok := res.(*frame.Error); ok {
				scb(_error)
				return
			}

			// get send receipt frame
			_, ok := res.(*frame.SendReceipt)
			if !ok {
				scb(fmt.Errorf("expected to receive a send receipt frame"))
				return
			}

			// call callback
			scb(nil)
		}
	}

	// send frame
	err := c.send(producer)
	if err != nil {
		return err
	}

	return nil
}

// CloseProducer will send a close producer request and call the provided
// callback with the response.
func (c *Client) CloseProducer(pid uint64, rcb func(error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	rid := c.requests
	c.requests++

	// create producer frame
	producer := &frame.CloseProducer{
		RID: rid,
		PID: pid,
	}

	// store callback
	if rcb != nil {
		c.requestCallbacks[rid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				rcb(err)
				return
			}

			// check for error frame
			if _error, ok := res.(*frame.Error); ok {
				rcb(_error)
				return
			}

			// get success frame
			_, ok := res.(*frame.Success)
			if !ok {
				rcb(fmt.Errorf("expected to receive a sucess frame"))
				return
			}

			// remove producer callback
			delete(c.producerCallbacks, pid) // TODO: Lock mutex?

			// call callback
			rcb(nil)
		}
	}

	// send frame
	err := c.send(producer)
	if err != nil {
		return err
	}

	return nil
}

// CreateConsumer will send a create consumer request and call the provided callback
// with the response. The second callback is called with ever incoming message
// for the created consumer.
func (c *Client) CreateConsumer(name, topic, sub string, typ frame.SubscriptionType, durable bool, rcb func(uint64, error), ccb func(*frame.Message, error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	rid := c.requests
	cid := c.consumers
	c.requests++
	c.consumers++

	// create subscribe frame
	subscribe := &frame.Subscribe{
		RID:          rid,
		CID:          cid,
		Name:         name,
		Topic:        topic,
		Subscription: sub,
		SubType:      typ,
		Durable:      durable,
		//StartMessageID
		//InitialPosition
	}

	// store request callback
	if rcb != nil {
		c.requestCallbacks[rid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				rcb(0, err)
				return
			}

			// check for error frame
			if _error, ok := res.(*frame.Error); ok {
				rcb(0, _error)
				return
			}

			// get success frame
			_, ok := res.(*frame.Success)
			if !ok {
				rcb(0, fmt.Errorf("expected to receive a success frame"))
				return
			}

			// call callback
			rcb(cid, nil)
		}
	}

	// TODO: Only store callback if request was successful?

	// store consumer callback
	if ccb != nil {
		c.consumerCallbacks[cid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				ccb(nil, err)
				return
			}

			// get message frame
			message, ok := res.(*frame.Message)
			if !ok {
				ccb(nil, fmt.Errorf("expected to receive a message frame"))
				return
			}

			// call callback
			ccb(message, nil)
		}
	}

	// send frame
	err := c.send(subscribe)
	if err != nil {
		return err
	}

	return nil
}

// Flow will send a flow request.
func (c *Client) Flow(cid uint64, num uint32) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// create flow frame
	flow := &frame.Flow{
		CID:      cid,
		Messages: num,
	}

	// send frame
	err := c.send(flow)
	if err != nil {
		return err
	}

	return nil
}

// Ack will send a ack request.
func (c *Client) Ack(cid uint64, typ frame.AckType, mid frame.MessageID) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// create ack frame
	ack := &frame.Ack{
		CID:         cid,
		AckType:     typ,
		MessagedIDs: []frame.MessageID{mid},
	}

	// send frame
	err := c.send(ack)
	if err != nil {
		return err
	}

	return nil
}

// CloseConsumer will send a close consumer request and call the provided
// callback with the response.
func (c *Client) CloseConsumer(cid uint64, rcb func(error)) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// increment counters
	rid := c.requests
	c.requests++

	// create consumer frame
	consumer := &frame.CloseConsumer{
		RID: rid,
		CID: cid,
	}

	// store request callback
	if rcb != nil {
		c.requestCallbacks[rid] = func(res frame.Frame, err error) {
			// handle error
			if err != nil {
				rcb(err)
				return
			}

			// check for error frame
			if _error, ok := res.(*frame.Error); ok {
				rcb(_error)
				return
			}

			// get success frame
			_, ok := res.(*frame.Success)
			if !ok {
				rcb(fmt.Errorf("expected to receive a sucess frame"))
				return
			}

			// remove consumer callback
			delete(c.consumerCallbacks, cid) // TODO: Lock mutex?

			// call callback
			rcb(nil)
		}
	}

	// send frame
	err := c.send(consumer)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the client.
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

func (c *Client) send(f frame.Frame) error {
	err := c.conn.Send(f)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) receiver() {
	// TODO: Cancel all callbacks on error.

	for {
		// receive next frame
		f, err := c.conn.Receive()
		if err != nil {
			// TODO: Handle error.
			return
		}

		// handle frame
		err = c.handleFrame(f)
		if err != nil {
			// TODO: Handle error.
			return
		}
	}
}

func (c *Client) handleFrame(f frame.Frame) error {
	// handle frame
	switch f.Type() {
	case frame.ConnectFrame:
		// not implemented
	case frame.ConnectedFrame:
		// not implemented
	case frame.SubscribeFrame:
		// not implemented
	case frame.ProducerFrame:
		// not implemented
	case frame.SendFrame:
		// not implemented
	case frame.SendReceiptFrame:
		sr := f.(*frame.SendReceipt)
		c.handleSendResponse(sr.PID, sr.Sequence, f)
	case frame.SendErrorFrame:
		se := f.(*frame.SendError)
		c.handleSendResponse(se.PID, se.Sequence, f)
	case frame.MessageFrame:
		c.handleConsumerResponse(f.(*frame.Message).CID, f)
	case frame.AckFrame:
		// not implemented
	case frame.FlowFrame:
		// not implemented
	case frame.UnsubscribeFrame:
		// not implemented
	case frame.SuccessFrame:
		c.handleRequestResponse(f.(*frame.Success).RID, f)
	case frame.ErrorFrame:
		c.handleRequestResponse(f.(*frame.Error).RID, f)
	case frame.CloseProducerFrame:
		c.handleProducerResponse(f.(*frame.CloseProducer).PID, f)
	case frame.CloseConsumerFrame:
		c.handleConsumerResponse(f.(*frame.CloseConsumer).CID, f)
	case frame.ProducerSuccessFrame:
		c.handleRequestResponse(f.(*frame.ProducerSuccess).RID, f)
	case frame.PingFrame:
		return c.handlePing()
	case frame.PongFrame:
		// not implemented
	case frame.RedeliverUnacknowledgedMessagesFrame:
		// not implemented
	case frame.PartitionedMetadataFrame:
		// not implemented
	case frame.PartitionedMetadataResponseFrame:
		// TODO: Implement.
	case frame.LookupFrame:
		// not implemented
	case frame.LookupResponseFrame:
		c.handleRequestResponse(f.(*frame.LookupResponse).RID, f)
	case frame.ConsumerStatsFrame:
		// not implemented
	case frame.ConsumerStatsResponseFrame:
		// TODO: Implement.
	case frame.ReachedEndOfTopicFrame:
		// TODO: Implement.
	case frame.SeekFrame:
		// not implemented
	case frame.GetLastMessageIDFrame:
		// not implemented
	case frame.GetLastMessageIDResponseFrame:
		// TODO: Implement.
	case frame.ActiveConsumerChangeFrame:
		// TODO: Implement.
	case frame.GetTopicsOfNamespaceFrame:
		// not implemented
	case frame.GetTopicsOfNamespaceResponseFrame:
		// TODO: Implement.
	case frame.GetSchemaFrame:
		// not implemented
	case frame.GetSchemaResponseFrame:
		// TODO: Implement.
	}

	return nil
}

func (c *Client) handleRequestResponse(rid uint64, f frame.Frame) {
	// acquire mutex
	c.mutex.Lock()

	// load callback
	cb, ok := c.requestCallbacks[rid]
	if !ok {
		return
	}

	// delete callback
	delete(c.requestCallbacks, rid)

	// release mutex
	c.mutex.Unlock()

	// call callback
	cb(f, nil)
}

func (c *Client) handleProducerResponse(pid uint64, f frame.Frame) {
	// acquire mutex
	c.mutex.Lock()

	// load callback
	cb, ok := c.producerCallbacks[pid]
	if !ok {
		return
	}

	// release mutex
	c.mutex.Unlock()

	// call callback
	cb(f, nil)
}

func (c *Client) handleSendResponse(pid, seq uint64, f frame.Frame) {
	// compute send key
	key := sendKey(pid, seq)

	// acquire mutex
	c.mutex.Lock()

	// load callback
	cb, ok := c.sendCallbacks[key]
	if !ok {
		return
	}

	// delete callback
	delete(c.sendCallbacks, key)

	// release mutex
	c.mutex.Unlock()

	// call callback
	cb(f, nil)
}

func (c *Client) handleConsumerResponse(cid uint64, f frame.Frame) {
	// acquire mutex
	c.mutex.Lock()

	// load callback
	cb, ok := c.consumerCallbacks[cid]
	if !ok {
		return
	}

	// release mutex
	c.mutex.Unlock()

	// call callback
	cb(f, nil)
}

func (c *Client) handlePing() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// send pong
	err := c.send(&frame.Pong{})
	if err != nil {
		return err
	}

	return nil
}

func sendKey(pid, seq uint64) string {
	return fmt.Sprintf("%d:%d", pid, seq)
}
