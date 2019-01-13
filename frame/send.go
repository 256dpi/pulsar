package frame

import (
	"time"

	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Send struct {
	ProducerName string
	PID          uint64
	Sequence     uint64
	Message      []byte
}

func (r *Send) Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error) {
	// prepare send command
	send := &pb.CommandSend{
		ProducerId:  proto.Uint64(r.PID),
		SequenceId:  proto.Uint64(r.Sequence),
		NumMessages: proto.Int32(1),
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_SEND),
		Send: send,
	}

	// prepare metadata
	metadata := &pb.MessageMetadata{
		ProducerName: proto.String(r.ProducerName),
		SequenceId:   proto.Uint64(r.Sequence),
		PublishTime:  proto.Uint64(uint64(time.Now().Unix())),
	}

	return base, metadata, r.Message, nil
}

type SendReceipt struct {
	PID       uint64
	Sequence  uint64
	MessageID MessageID
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

func (r *SendError) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.PID = bc.SendError.GetProducerId()
	r.Sequence = bc.SendError.GetSequenceId()
	r.Error = pb.ServerError_name[int32(bc.SendError.GetError())]
	r.Message = bc.SendError.GetMessage()

	return nil
}
