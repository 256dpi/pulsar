package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/pb"
)

// Error is received from the broker when a request failed.
type Error struct {
	// RID is the request id.
	RID uint64

	// Code is the error code.
	Code string

	// Message is an additional error message
	Message string
}

// Type will return the frame type.
func (e *Error) Type() Type {
	return ERROR
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("pulsar: %s: %s", e.Code, e.Message)
	} else {
		return fmt.Sprintf("pulsar: %s", e.Code)
	}
}

// Decode will construct the frame from the specified components.
func (e *Error) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.RID = bc.Error.GetRequestId()
	e.Code = pb.ServerError_name[int32(bc.Error.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
