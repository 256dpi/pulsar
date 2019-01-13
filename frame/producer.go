package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Producer struct {
	RID   uint64
	ID    uint64
	Name  string
	Topic string
}

func (p *Producer) Type() Type {
	return PRODUCER
}

func (p *Producer) Encode() (*pb.BaseCommand, error) {
	// prepare producer command
	producer := &pb.CommandProducer{}
	producer.Topic = proto.String(p.Topic)
	producer.ProducerId = proto.Uint64(p.ID)
	producer.RequestId = proto.Uint64(p.RID)
	producer.ProducerName = proto.String(p.Name)
	//producer.Encrypted
	//producer.Metadata
	//producer.Schema

	// prepare base command
	base := &pb.BaseCommand{
		Type:     getType(pb.BaseCommand_PRODUCER),
		Producer: producer,
	}

	return base, nil
}

type ProducerSuccess struct {
	RID          uint64
	Name         string
	LastSequence int64
}

func (s *ProducerSuccess) Type() Type {
	return PRODUCER_SUCCESS
}

func (s *ProducerSuccess) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.RID = bc.ProducerSuccess.GetRequestId()
	s.Name = bc.ProducerSuccess.GetProducerName()
	s.LastSequence = bc.ProducerSuccess.GetLastSequenceId()

	return nil
}
