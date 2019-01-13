package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type AckType int

const (
	Individual = pb.CommandAck_Individual
	Cumulative = pb.CommandAck_Cumulative
)

type Ack struct {
	ConsumerID uint64
	AckType    AckType
	MessagedID MessageID
}

func (a *Ack) Type() Type {
	return ACK
}

func (a *Ack) Encode() (*pb.BaseCommand, error) {
	// prepare ack type
	ackType := pb.CommandAck_AckType(a.AckType)

	// prepare ack command
	ack := &pb.CommandAck{}
	ack.ConsumerId = proto.Uint64(a.ConsumerID)
	ack.AckType = &ackType

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_ACK),
		Ack:  ack,
	}

	return base, nil
}
