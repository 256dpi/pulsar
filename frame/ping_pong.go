package frame

import "github.com/256dpi/pulsar/api"

// Ping is a ping message sent by the broker.
type Ping struct{}

// Type will return the frame type.
func (p *Ping) Type() Type {
	return PingFrame
}

// Decode will construct the frame from the specified components.
func (p *Ping) Decode(bc *api.BaseCommand) error {
	return nil
}

// Encode will encode the frame and return its components.
func (p *Ping) Encode() (*api.BaseCommand, error) {
	// prepare base command
	base := &api.BaseCommand{
		Type: getType(api.BaseCommand_PING),
		Ping: &api.CommandPing{},
	}

	return base, nil
}

// Pong is a pong message sent by the client.
type Pong struct{}

// Type will return the frame type.
func (p *Pong) Type() Type {
	return PongFrame
}

// Decode will construct the frame from the specified components.
func (p *Pong) Decode(bc *api.BaseCommand) error {
	return nil
}

// Encode will encode the frame and return its components.
func (p *Pong) Encode() (*api.BaseCommand, error) {
	// prepare base command
	base := &api.BaseCommand{
		Type: getType(api.BaseCommand_PONG),
		Pong: &api.CommandPong{},
	}

	return base, nil
}
