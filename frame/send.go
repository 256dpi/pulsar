package frame

import "github.com/256dpi/pulsar/pb"

type Send struct {
	ProducerID uint64
	SequenceID uint64
}

func (r *Send) Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error) {
	return nil, nil, nil, nil
}

// TODO: Add batch.

type SendReceipt struct {
	ProducerID uint64
	SequenceID uint64
	MessageID  MessageID
}

func (r *SendReceipt) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.ProducerID = bc.SendReceipt.GetProducerId()
	r.SequenceID = bc.SendReceipt.GetSequenceId()
	r.MessageID = convertMessageId(bc.SendReceipt.MessageId)

	return nil
}

type SendError struct {
	ProducerID uint64
	SequenceID uint64
	Error      string
	Message    string
}

func (r *SendError) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.ProducerID = bc.SendError.GetProducerId()
	r.SequenceID = bc.SendError.GetSequenceId()
	r.Error = pb.ServerError_name[int32(bc.SendError.GetError())]
	r.Message = bc.SendError.GetMessage()

	return nil
}
