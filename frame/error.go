package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/api"
)

// Error is received from the broker when a request failed.
type Error struct {
	// The request id.
	RID uint64

	// The error code.
	Code string

	// The error message.
	Message string
}

// Type will return the frame type.
func (e *Error) Type() Type {
	return ErrorFrame
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("pulsar: %s: %s", e.Code, e.Message)
	}

	return fmt.Sprintf("pulsar: %s", e.Code)
}

// Decode will construct the frame from the specified components.
func (e *Error) Decode(bc *api.BaseCommand) error {
	// set fields
	e.RID = bc.Error.GetRequestId()
	e.Code = api.ServerError_name[int32(bc.Error.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
