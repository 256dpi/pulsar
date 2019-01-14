package frame

import (
	"time"

	"github.com/256dpi/pulsar/api"

	"github.com/golang/protobuf/proto"
)

// TODO: Encode all metadata?
// TODO: Split in MessageMetadata and SendMetadata?

// Metadata is data associated with sent and received message.
type Metadata struct {
	Sequence     uint64
	ProducerName string
	PublishTime  time.Time
	EventTime    time.Time

	// TODO: Support properties.
	// TODO: Support replication.
	// TODO: Support partition.
	// TODO: Support compression.
	// TODO: Support batch.
	// TODO: Support encryption.
	// TODO: Support schema.
}

func decodeMetadata(md *api.MessageMetadata) Metadata {
	// check availability
	if md == nil {
		return Metadata{}
	}

	return Metadata{
		Sequence:     md.GetSequenceId(),
		ProducerName: md.GetProducerName(),
		PublishTime:  time.Unix(int64(md.GetPublishTime()), 0),
		EventTime:    time.Unix(int64(md.GetEventTime()), 0),
	}
}

func encodeMetadata(m Metadata) *api.MessageMetadata {
	return &api.MessageMetadata{
		SequenceId:   proto.Uint64(m.Sequence),
		ProducerName: proto.String(m.ProducerName),
		PublishTime:  proto.Uint64(uint64(m.PublishTime.Unix())),
		EventTime:    proto.Uint64(uint64(m.EventTime.Unix())),
	}
}
