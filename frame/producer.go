package frame

import (
	"github.com/256dpi/pulsar/pb"
	"github.com/golang/protobuf/proto"
)

type Producer struct {
	ID        uint64
	Name      string
	Topic     string
	RequestID uint64
}

func (p *Producer) Encode() (*pb.BaseCommand, error) {
	// prepare producer command
	producer := &pb.CommandProducer{}
	producer.Topic = proto.String(p.Topic)
	producer.ProducerId = proto.Uint64(p.ID)
	producer.RequestId = proto.Uint64(p.RequestID)
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
	Name         string
	LastSequence int64
	RequestID    uint64
}

func (s *ProducerSuccess) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.Name = bc.ProducerSuccess.GetProducerName()
	s.RequestID = bc.ProducerSuccess.GetRequestId()
	s.LastSequence = bc.ProducerSuccess.GetLastSequenceId()

	return nil
}
