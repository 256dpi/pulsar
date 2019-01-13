package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Flow struct {
	CID            uint64
	MessagePermits uint32
}

func (f *Flow) Encode() (*pb.BaseCommand, error) {
	// prepare flow command
	flow := &pb.CommandFlow{
		ConsumerId:     proto.Uint64(f.CID),
		MessagePermits: proto.Uint32(f.MessagePermits),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_FLOW),
		Flow: flow,
	}

	return base, nil
}
