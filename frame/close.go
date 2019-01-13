package frame

import "github.com/256dpi/pulsar/pb"

type CloseProducer struct {
}

type CloseConsumer struct {
}

type Success struct {
	RequestID uint64
}

func (e *Success) Decode(bc *pb.BaseCommand) error {
	// set fields
	e.RequestID = bc.Success.GetRequestId()

	return nil
}
