package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Unsubscribe struct {
	RID uint64
	CID uint64
}

func (s *Unsubscribe) Encode() (*pb.BaseCommand, error) {
	// prepare unsubscribe command
	unsubscribe := &pb.CommandUnsubscribe{
		RequestId:  proto.Uint64(s.RID),
		ConsumerId: proto.Uint64(s.CID),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type:        getType(pb.BaseCommand_UNSUBSCRIBE),
		Unsubscribe: unsubscribe,
	}

	return base, nil
}
