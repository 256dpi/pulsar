package frame

import "github.com/256dpi/pulsar/pb"

type Ping struct{}

// Type will return the frame type.
func (p *Ping) Type() Type {
	return PING
}

// Decode will construct the frame from the specified components.
func (p *Ping) Decode(bc *pb.BaseCommand) error {
	return nil
}

// Encode will encode the frame and return its components.
func (p *Ping) Encode() (*pb.BaseCommand, error) {
	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_PING),
		Ping: &pb.CommandPing{},
	}

	return base, nil
}

type Pong struct{}

// Type will return the frame type.
func (p *Pong) Type() Type {
	return PONG
}

// Decode will construct the frame from the specified components.
func (p *Pong) Decode(bc *pb.BaseCommand) error {
	return nil
}

// Encode will encode the frame and return its components.
func (p *Pong) Encode() (*pb.BaseCommand, error) {
	// prepare base command
	base := &pb.BaseCommand{
		Type: getType(pb.BaseCommand_PONG),
		Pong: &pb.CommandPong{},
	}

	return base, nil
}
