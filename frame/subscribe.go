package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type SubscriptionType int

const (
	Exclusive = SubscriptionType(pb.CommandSubscribe_Exclusive)
	Shared    = SubscriptionType(pb.CommandSubscribe_Shared)
	Failover  = SubscriptionType(pb.CommandSubscribe_Failover)
)

type InitialPosition int

const (
	Latest   = InitialPosition(pb.CommandSubscribe_Latest)
	Earliest = InitialPosition(pb.CommandSubscribe_Earliest)
)

type Subscribe struct {
	RID             uint64
	CID             uint64
	Name            string
	Topic           string
	Subscription    string
	SubType         SubscriptionType
	PriorityLevel   int32
	Durable         bool
	StartMessageID  *MessageID
	InitialPosition InitialPosition
}

// Type will return the frame type.
func (s *Subscribe) Type() Type {
	return SUBSCRIBE
}

// Encode will encode the frame and return its components.
func (s *Subscribe) Encode() (*pb.BaseCommand, error) {
	// prepare sub type
	subType := pb.CommandSubscribe_SubType(int32(s.SubType))
	inPos := pb.CommandSubscribe_InitialPosition(int32(s.InitialPosition))

	// prepare subscribe command
	subscribe := &pb.CommandSubscribe{
		RequestId:     proto.Uint64(s.RID),
		ConsumerId:    proto.Uint64(s.CID),
		ConsumerName:  proto.String(s.Name),
		Topic:         proto.String(s.Topic),
		Subscription:  proto.String(s.Subscription),
		SubType:       &subType,
		PriorityLevel: proto.Int32(s.PriorityLevel),
		Durable:       proto.Bool(s.Durable),
		// StartMessageId
		InitialPosition: &inPos,
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type:      getType(pb.BaseCommand_SUBSCRIBE),
		Subscribe: subscribe,
	}

	return base, nil
}
