package frame

import "github.com/256dpi/pulsar/pb"

// Message is an incoming message received by a consumer.
type Message struct {
	// The consumer id.
	CID uint64

	// The message id.
	MessageID MessageID

	// The redelivery count.
	RedeliveryCount uint32

	// The message metadata.
	Metadata Metadata

	// The message payload.
	Payload []byte
}

// Type will return the frame type.
func (m *Message) Type() Type {
	return MessageFrame
}

// Decode will construct the frame from the specified components.
func (m *Message) Decode(bc *pb.BaseCommand, md *pb.MessageMetadata, payload []byte) error {
	// set fields
	m.CID = bc.Message.GetConsumerId()
	m.MessageID = decodeMessageID(bc.Message.MessageId)
	m.RedeliveryCount = bc.Message.GetRedeliveryCount()
	m.Metadata = decodeMetadata(md)
	m.Payload = payload

	return nil
}
