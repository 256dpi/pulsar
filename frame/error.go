package frame

import "github.com/256dpi/pulsar/pb"

type Error struct {
	RID     uint64
	Error   string
	Message string
}

// Type will return the frame type.
func (e *Error) Type() Type {
	return ERROR
}

// Decode will construct the frame from the specified components.
func (e *Error) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.RID = bc.Error.GetRequestId()
	e.Error = pb.ServerError_name[int32(bc.Error.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
