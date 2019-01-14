package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

var authMethodNone = pb.AuthMethod_AuthMethodNone
var protocolVersion = int32(pb.ProtocolVersion_v13)

// Connect is sent to the broker to initiate a connection.
type Connect struct {
	// The identifier set by the client.
	ClientVersion string

	// The URL of the broker that the client should be proxied to.
	ProxyBrokerURL string

	// TODO: Support auth methods and data.
	// TODO: Support for original principal and auth.
}

// Type will return the frame type.
func (c *Connect) Type() Type {
	return ConnectFrame
}

// Encode will encode the frame and return its components.
func (c *Connect) Encode() (*pb.BaseCommand, error) {
	// prepare connect command
	connect := &pb.CommandConnect{}

	// set fields
	connect.ClientVersion = proto.String(c.ClientVersion)
	connect.AuthMethod = &authMethodNone
	connect.ProtocolVersion = &protocolVersion

	// set proxy url if available
	if c.ProxyBrokerURL != "" {
		connect.ProxyToBrokerUrl = proto.String(c.ProxyBrokerURL)
	}

	// prepare base command
	base := &pb.BaseCommand{
		Type:    getType(pb.BaseCommand_CONNECT),
		Connect: connect,
	}

	return base, nil
}

// Connected is received by the broker to acknowledge a connection.
type Connected struct {
	// The identifier set by the broker.
	ServerVersion string
}

// Type will return the frame type.
func (c *Connected) Type() Type {
	return ConnectedFrame
}

// Decode will construct the frame from the specified components.
func (c *Connected) Decode(bc *pb.BaseCommand) error {
	// check protocol version
	if bc.Connected.GetProtocolVersion() != int32(pb.ProtocolVersion_v13) {
		return fmt.Errorf("only protocol version 13 is supported")
	}

	// set fields
	c.ServerVersion = bc.Connected.GetServerVersion()

	return nil
}
