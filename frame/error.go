package frame

import "github.com/256dpi/pulsar/pb"

type Error struct {
	RequestID uint64
	Error     string
	Message   string
}

func (e *Error) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.RequestID = bc.Error.GetRequestId()
	e.Error = pb.ServerError_name[int32(bc.Error.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
