package frame

import "github.com/256dpi/pulsar/pb"

type Producer struct {
	Topic        string
	ProducerID   int
	RequestID    int
	ProducerName string
}

type ProducerSuccess struct {
	RequestID      uint64
	ProducerName   string
	LastSequenceID int64
}

func (s *ProducerSuccess) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.RequestID = bc.ProducerSuccess.GetRequestId()
	s.ProducerName = bc.ProducerSuccess.GetProducerName()
	s.LastSequenceID = bc.ProducerSuccess.GetLastSequenceId()

	return nil
}
