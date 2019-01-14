package frame

import (
	"fmt"
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// Send will send a message to the broker.
type Send struct {
	// The producer id.
	PID uint64

	// The sequence.
	Sequence uint64

	// The message metadata.
	Metadata Metadata

	// The message.
	Message []byte
}

// Type will return the frame type.
func (s *Send) Type() Type {
	return SEND
}

// Encode will encode the frame and return its components.
func (s *Send) Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error) {
	// prepare send command
	send := &pb.CommandSend{
		ProducerId:  proto.Uint64(s.PID),
		SequenceId:  proto.Uint64(s.Sequence),
		NumMessages: proto.Int32(1),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_SEND),
		Send: send,
	}

	// prepare metadata
	metadata := encodeMetadata(s.Metadata)

	return base, metadata, s.Message, nil
}

// SendReceipt is received by the client if a message has been successfully
// produced.
type SendReceipt struct {
	// The producer id
	PID uint64

	// The message sequence.
	Sequence uint64

	// The message id.
	MessageID MessageID
}

// Type will return the frame type.
func (r *SendReceipt) Type() Type {
	return SEND_RECEIPT
}

// Decode will construct the frame from the specified components.
func (r *SendReceipt) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.PID = bc.SendReceipt.GetProducerId()
	r.Sequence = bc.SendReceipt.GetSequenceId()
	r.MessageID = decodeMessageID(bc.SendReceipt.MessageId)

	return nil
}

// SendError is received by the client if the broker failed to produce a message.
type SendError struct {
	// The producer id.
	PID uint64

	// The message sequence.
	Sequence uint64

	// The error code.
	Code string

	// The error message.
	Message string
}

// Type will return the frame type.
func (e *SendError) Type() Type {
	return SEND_ERROR
}

// Error implements the error interface.
func (e *SendError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("pulsar: %s: %s", e.Code, e.Message)
	} else {
		return fmt.Sprintf("pulsar: %s", e.Code)
	}
}

// Decode will construct the frame from the specified components.
func (e *SendError) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.PID = bc.SendError.GetProducerId()
	e.Sequence = bc.SendError.GetSequenceId()
	e.Code = pb.ServerError_name[int32(bc.SendError.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
