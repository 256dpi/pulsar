package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// Flow instructs the broker to send up to the specified amount of messages.
type Flow struct {
	// The consumer id.
	CID uint64

	// The number of messages to request.
	Messages uint32
}

// Type will return the frame type.
func (f *Flow) Type() Type {
	return FlowFrame
}

// Encode will encode the frame and return its components.
func (f *Flow) Encode() (*pb.BaseCommand, error) {
	// prepare flow command
	flow := &pb.CommandFlow{
		ConsumerId:     proto.Uint64(f.CID),
		MessagePermits: proto.Uint32(f.Messages),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_FLOW),
		Flow: flow,
	}

	return base, nil
}
