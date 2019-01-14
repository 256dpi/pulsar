package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// Producer is sent to the broker to create a producer.
type Producer struct {
	// The request id.
	RID uint64

	// The producer id.
	PID uint64

	// The producer name.
	Name string

	// The topic.
	Topic string

	// TODO: Support encrypted.
	// TODO: Support metadata.
	// TODO: Support schema.
}

// Type will return the frame type.
func (p *Producer) Type() Type {
	return PRODUCER
}

// Encode will encode the frame and return its components.
func (p *Producer) Encode() (*pb.BaseCommand, error) {
	// prepare producer command
	producer := &pb.CommandProducer{}
	producer.Topic = proto.String(p.Topic)
	producer.ProducerId = proto.Uint64(p.PID)
	producer.RequestId = proto.Uint64(p.RID)
	producer.ProducerName = proto.String(p.Name)

	// prepare base command
	base := &pb.BaseCommand{
		Type:     getType(pb.BaseCommand_PRODUCER),
		Producer: producer,
	}

	return base, nil
}

// ProducerSuccess is received as a response to the Producer request.
type ProducerSuccess struct {
	// The request id.
	RID uint64

	// The producer name.
	Name string

	// The last sequence.
	LastSequence int64

	// TODO: Support schema version.
}

// Type will return the frame type.
func (s *ProducerSuccess) Type() Type {
	return PRODUCER_SUCCESS
}

// Decode will construct the frame from the specified components.
func (s *ProducerSuccess) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.RID = bc.ProducerSuccess.GetRequestId()
	s.Name = bc.ProducerSuccess.GetProducerName()
	s.LastSequence = bc.ProducerSuccess.GetLastSequenceId()

	return nil
}
