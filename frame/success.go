package frame

import "github.com/256dpi/pulsar/pb"

type Success struct {
	RID uint64
}

// Type will return the frame type.
func (s *Success) Type() Type {
	return SUCCESS
}

// Decode will construct the frame from the specified components.
func (s *Success) Decode(bc *pb.BaseCommand) error {
	// set fields
	s.RID = bc.Success.GetRequestId()

	return nil
}
