package frame

import "github.com/256dpi/pulsar/pb"

type Success struct {
	RID uint64
}

func (s *Success) Type() Type {
	return SUCCESS
}

func (s *Success) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.RID = bc.Success.GetRequestId()

	return nil
}
