package frame

import "github.com/256dpi/pulsar/pb"

type Ping struct{}

func (p *Ping) Decode(bc *pb.BaseCommand) error {
	return nil
}

func (p *Ping) Encode() (*pb.BaseCommand, error) {
	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_PING),
		Ping: &pb.CommandPing{},
	}

	return base, nil
}

type Pong struct{}

func (p *Pong) Decode(bc *pb.BaseCommand) error {
	return nil
}

func (p *Pong) Encode() (*pb.BaseCommand, error) {
	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_PONG),
		Pong: &pb.CommandPong{},
	}

	return base, nil
}
