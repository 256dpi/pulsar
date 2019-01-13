package frame

import "github.com/256dpi/pulsar/pb"

func getType(t pb.BaseCommand_Type) *pb.BaseCommand_Type {
	return &t
}
