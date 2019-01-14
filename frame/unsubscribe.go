package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// Unsubscribe will remove the subscription from the broker.
type Unsubscribe struct {
	// The request id.
	RID uint64

	// The consumer id.
	CID uint64
}

// Type will return the frame type.
func (u *Unsubscribe) Type() Type {
	return UnsubscribeFrame
}

// Encode will encode the frame and return its components.
func (u *Unsubscribe) Encode() (*pb.BaseCommand, error) {
	// prepare unsubscribe command
	unsubscribe := &pb.CommandUnsubscribe{
		RequestId:  proto.Uint64(u.RID),
		ConsumerId: proto.Uint64(u.CID),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type:        getType(pb.BaseCommand_UNSUBSCRIBE),
		Unsubscribe: unsubscribe,
	}

	return base, nil
}
