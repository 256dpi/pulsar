package frame

import "github.com/256dpi/pulsar/api"

// Success is sent by the broker to acknowledge requests.
type Success struct {
	// The request id.
	RID uint64
}

// Type will return the frame type.
func (s *Success) Type() Type {
	return SuccessFrame
}

// Decode will construct the frame from the specified components.
func (s *Success) Decode(bc *api.BaseCommand) error {
	// set fields
	s.RID = bc.Success.GetRequestId()

	return nil
}
