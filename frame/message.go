package frame

import "github.com/256dpi/pulsar/pb"

type Message struct {
	ConsumerID int
	MessageID  MessageID
}

func (m *Message) Decode(*pb.BaseCommand, *pb.MessageMetadata, []byte) error {
	return nil
}

// TODO: Plus payload.
