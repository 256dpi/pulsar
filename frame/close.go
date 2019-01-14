package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type CloseProducer struct {
	PID uint64
	RID uint64
}

// Type will return the frame type.
func (p *CloseProducer) Type() Type {
	return CLOSE_PRODUCER
}

// Encode will encode the frame and return its components.
func (p *CloseProducer) Encode() (*pb.BaseCommand, error) {
	// prepare close producer command
	closeProducer := &pb.CommandCloseProducer{}

	// set fields
	closeProducer.ProducerId = proto.Uint64(p.PID)
	closeProducer.RequestId = proto.Uint64(p.RID)

	// prepare base command
	base := &pb.BaseCommand{
		Type:          getType(pb.BaseCommand_CLOSE_PRODUCER),
		CloseProducer: closeProducer,
	}

	return base, nil
}

// Decode will construct the frame from the specified components.
func (p *CloseProducer) Decode(bc *pb.BaseCommand) error {
	// set fields
	p.PID = bc.CloseProducer.GetProducerId()
	p.RID = bc.CloseProducer.GetRequestId()

	return nil
}

type CloseConsumer struct {
	CID uint64
	RID uint64
}

// Type will return the frame type.
func (c *CloseConsumer) Type() Type {
	return CLOSE_CONSUMER
}

// Encode will encode the frame and return its components.
func (c *CloseConsumer) Encode() (*pb.BaseCommand, error) {
	// prepare close consumer command
	closeConsumer := &pb.CommandCloseConsumer{}

	// set fields
	closeConsumer.ConsumerId = proto.Uint64(c.CID)
	closeConsumer.RequestId = proto.Uint64(c.RID)

	// prepare base command
	base := &pb.BaseCommand{
		Type:          getType(pb.BaseCommand_CLOSE_CONSUMER),
		CloseConsumer: closeConsumer,
	}

	return base, nil
}

// Decode will construct the frame from the specified components.
func (c *CloseConsumer) Decode(bc *pb.BaseCommand) error {
	// set fields
	c.CID = bc.CloseConsumer.GetConsumerId()
	c.RID = bc.CloseConsumer.GetRequestId()

	return nil
}
