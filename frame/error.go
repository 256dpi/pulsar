package frame

import "github.com/256dpi/pulsar/pb"

type Error struct {
	RID     uint64
	Error   string
	Message string
}

func (e *Error) Type() Type {
	return ERROR
}

func (e *Error) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.RID = bc.Error.GetRequestId()
	e.Error = pb.ServerError_name[int32(bc.Error.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
