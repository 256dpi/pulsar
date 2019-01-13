package frame

import (
	"time"

	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Send struct {
	PID          uint64
	Sequence     uint64
	ProducerName string
	Message      []byte
}

func (s *Send) Type() Type {
	return SEND
}

func (s *Send) Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error) {
	// prepare send command
	send := &pb.CommandSend{
		ProducerId:  proto.Uint64(s.PID),
		SequenceId:  proto.Uint64(s.Sequence),
		NumMessages: proto.Int32(1),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_SEND),
		Send: send,
	}

	// prepare metadata
	metadata := &pb.MessageMetadata{
		ProducerName: proto.String(s.ProducerName),
		SequenceId:   proto.Uint64(s.Sequence),
		PublishTime:  proto.Uint64(uint64(time.Now().Unix())),
	}

	return base, metadata, s.Message, nil
}

type SendReceipt struct {
	PID       uint64
	Sequence  uint64
	MessageID MessageID
}

func (r *SendReceipt) Type() Type {
	return SEND_RECEIPT
}

func (r *SendReceipt) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.PID = bc.SendReceipt.GetProducerId()
	r.Sequence = bc.SendReceipt.GetSequenceId()
	r.MessageID = convertMessageId(bc.SendReceipt.MessageId)

	return nil
}

type SendError struct {
	PID      uint64
	Sequence uint64
	Error    string
	Message  string
}

func (e *SendError) Type() Type {
	return SEND_ERROR
}

func (e *SendError) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.PID = bc.SendError.GetProducerId()
	e.Sequence = bc.SendError.GetSequenceId()
	e.Error = pb.ServerError_name[int32(bc.SendError.GetError())]
	e.Message = bc.SendError.GetMessage()

	return nil
}
