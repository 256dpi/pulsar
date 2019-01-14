package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// AckType defines the type of the acknowledge operation.
type AckType int

const (
	// Individual will acknowledge the single specified message.
	Individual = AckType(pb.CommandAck_Individual)

	// Cumulative will acknowledge all messages including the specified message.
	Cumulative = AckType(pb.CommandAck_Cumulative)
)

// Ack is the frame sent to acknowledge a received message.
type Ack struct {
	// The consumer id.
	CID uint64

	// The acknowledgment type.
	AckType AckType

	// The message ids to acknowledge.
	MessagedIDs []MessageID

	// TODO: Support validation error.
	// TODO: Support properties.
}

// Type will return the frame type.
func (a *Ack) Type() Type {
	return AckFrame
}

// Encode will encode the frame and return its components.
func (a *Ack) Encode() (*pb.BaseCommand, error) {
	// prepare ack type
	ackType := pb.CommandAck_AckType(a.AckType)

	// prepare ack command
	ack := &pb.CommandAck{}
	ack.ConsumerId = proto.Uint64(a.CID)
	ack.AckType = &ackType
	ack.MessageId = make([]*pb.MessageIdData, 0, len(a.MessagedIDs))

	// add message ids
	for _, mid := range a.MessagedIDs {
		ack.MessageId = append(ack.MessageId, encodeMessageID(mid))
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_ACK),
		Ack:  ack,
	}

	return base, nil
}
