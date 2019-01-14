package frame

import (
	"github.com/256dpi/pulsar/api"

	"github.com/golang/protobuf/proto"
)

// MessageID is the id of a single message.
type MessageID struct {
	LedgerID uint64
	EntryID  uint64

	// TODO: Support partition.
	// TODO: Support batch index.
}

func decodeMessageID(mid *api.MessageIdData) MessageID {
	// check availability
	if mid == nil {
		return MessageID{}
	}

	return MessageID{
		LedgerID: mid.GetLedgerId(),
		EntryID:  mid.GetEntryId(),
	}
}

func encodeMessageID(m MessageID) *api.MessageIdData {
	return &api.MessageIdData{
		LedgerId: proto.Uint64(m.LedgerID),
		EntryId:  proto.Uint64(m.EntryID),
	}
}
