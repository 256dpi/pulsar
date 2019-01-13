package frame

import "github.com/256dpi/pulsar/pb"

type Message struct {
	CID             uint64
	MessageID       MessageID
	RedeliveryCount uint32
	Sequence        uint64
	ProducerName    string
	Message         []byte
}

func (m *Message) Type() Type {
	return MESSAGE
}

func (m *Message) Decode(bc *pb.BaseCommand, md *pb.MessageMetadata, payload []byte) error {
	// set fields
	m.CID = bc.Message.GetConsumerId()
	m.MessageID = convertMessageId(bc.Message.MessageId)
	m.RedeliveryCount = bc.Message.GetRedeliveryCount()
	m.Sequence = md.GetSequenceId()
	m.ProducerName = md.GetProducerName()
	m.Message = payload

	return nil
}
