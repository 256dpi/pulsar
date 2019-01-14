package frame

import (
	"github.com/256dpi/pulsar/pb"
	
	"github.com/golang/protobuf/proto"
)

type MessageID struct {
	LedgerID   uint64
	EntryID    uint64
	Partition  int32
	BatchIndex int32
}

func decodeMessageID(mid *pb.MessageIdData) MessageID {
	// check availability
	if mid == nil {
		return MessageID{}
	}

	return MessageID{
		LedgerID:   mid.GetLedgerId(),
		EntryID:    mid.GetEntryId(),
		Partition:  mid.GetPartition(),
		BatchIndex: mid.GetBatchIndex(),
	}
}

func encodeMessageID(m MessageID) *pb.MessageIdData {
	return &pb.MessageIdData{
		LedgerId:   proto.Uint64(m.LedgerID),
		EntryId:    proto.Uint64(m.EntryID),
		Partition:  proto.Int32(m.Partition),
		BatchIndex: proto.Int32(m.BatchIndex),
	}
}
