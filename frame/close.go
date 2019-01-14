package frame

import (
	"github.com/256dpi/pulsar/api"

	"github.com/golang/protobuf/proto"
)

// CloseProducer is sent to close a producer or received if a producer has been
// closed by the broker.
type CloseProducer struct {
	// The request id.
	RID uint64

	// The producer id.
	PID uint64
}

// Type will return the frame type.
func (p *CloseProducer) Type() Type {
	return CloseProducerFrame
}

// Encode will encode the frame and return its components.
func (p *CloseProducer) Encode() (*api.BaseCommand, error) {
	// prepare close producer command
	closeProducer := &api.CommandCloseProducer{}

	// set fields
	closeProducer.ProducerId = proto.Uint64(p.PID)
	closeProducer.RequestId = proto.Uint64(p.RID)

	// prepare base command
	base := &api.BaseCommand{
		Type:          getType(api.BaseCommand_CLOSE_PRODUCER),
		CloseProducer: closeProducer,
	}

	return base, nil
}

// Decode will construct the frame from the specified components.
func (p *CloseProducer) Decode(bc *api.BaseCommand) error {
	// set fields
	p.PID = bc.CloseProducer.GetProducerId()
	p.RID = bc.CloseProducer.GetRequestId()

	return nil
}

// CloseConsumer is sent to close a consumer or received if a consumer has been
// closed by the broker.
type CloseConsumer struct {
	// The request id.
	RID uint64

	// The consumer id.
	CID uint64
}

// Type will return the frame type.
func (c *CloseConsumer) Type() Type {
	return CloseConsumerFrame
}

// Encode will encode the frame and return its components.
func (c *CloseConsumer) Encode() (*api.BaseCommand, error) {
	// prepare close consumer command
	closeConsumer := &api.CommandCloseConsumer{}

	// set fields
	closeConsumer.ConsumerId = proto.Uint64(c.CID)
	closeConsumer.RequestId = proto.Uint64(c.RID)

	// prepare base command
	base := &api.BaseCommand{
		Type:          getType(api.BaseCommand_CLOSE_CONSUMER),
		CloseConsumer: closeConsumer,
	}

	return base, nil
}

// Decode will construct the frame from the specified components.
func (c *CloseConsumer) Decode(bc *api.BaseCommand) error {
	// set fields
	c.CID = bc.CloseConsumer.GetConsumerId()
	c.RID = bc.CloseConsumer.GetRequestId()

	return nil
}
