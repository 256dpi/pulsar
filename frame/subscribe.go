package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

// SubscriptionType defines the subscription type.
type SubscriptionType int

const (
	// Exclusive subscriptions are only allowed to be subscribed by one client.
	// Additional subscriptions will return an error.
	Exclusive = SubscriptionType(pb.CommandSubscribe_Exclusive)

	// Shared subscriptions allow messages to be distributed among the consumers.
	Shared = SubscriptionType(pb.CommandSubscribe_Shared)

	// Failover subscriptions allow additional consumers to take over when the
	// active consumer fail.s
	Failover = SubscriptionType(pb.CommandSubscribe_Failover)
)

// InitialPosition defines the initial position of a subscription.
type InitialPosition int

const (
	// Latest will begin consuming messages from the latest message.
	Latest = InitialPosition(pb.CommandSubscribe_Latest)

	// Earliest will begin consuming messages from the earliest message.
	Earliest = InitialPosition(pb.CommandSubscribe_Earliest)
)

// Subscribe is sent to the broker to create a consumer.
type Subscribe struct {
	// The request id.
	RID uint64

	// The consumer id.
	CID uint64

	// The consumer name.
	Name string

	// The topic.
	Topic string

	// The subscription name.
	Subscription string

	// The subscription type.
	//
	// Default: Exclusive.
	SubType SubscriptionType

	// The durable flag.
	Durable bool

	// The initial position for the subscription.
	//
	// Default: Latest.
	InitialPosition InitialPosition

	// TODO: Support priority level.
	// TODO: Support start message id.
	// TODO: Support metadata.
	// TODO: Support compacted read.
	// TODO: Support schema.
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
		RequestId:       proto.Uint64(s.RID),
		ConsumerId:      proto.Uint64(s.CID),
		ConsumerName:    proto.String(s.Name),
		Topic:           proto.String(s.Topic),
		Subscription:    proto.String(s.Subscription),
		SubType:         &subType,
		Durable:         proto.Bool(s.Durable),
		InitialPosition: &inPos,
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type:      getType(pb.BaseCommand_SUBSCRIBE),
		Subscribe: subscribe,
	}

	return base, nil
}
