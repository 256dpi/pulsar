package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Unsubscribe struct {
	RID uint64
	CID uint64
}

// Type will return the frame type.
func (u *Unsubscribe) Type() Type {
	return UNSUBSCRIBE
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
