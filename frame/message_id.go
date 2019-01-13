package frame

import "github.com/256dpi/pulsar/pb"

type MessageID struct {
	LedgerID   uint64
	EntryID    uint64
	Partition  int32
	BatchIndex int32
}

func convertMessageId(mid *pb.MessageIdData) MessageID {
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
