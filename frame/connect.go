package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/api"

	"github.com/golang/protobuf/proto"
)

var authMethodNone = api.AuthMethod_AuthMethodNone
var protocolVersion = int32(api.ProtocolVersion_v13)

// Connect is sent to the broker to initiate a connection.
type Connect struct {
	// The client version identifier set by the client.
	ClientVersion string

	// The URL of the broker that the client should be connected to if a proxy
	// is used.
	ProxyTargetBrokerURL string

	// TODO: Support auth methods and data.
	// TODO: Support for original principal and auth.
}

// Type will return the frame type.
func (c *Connect) Type() Type {
	return ConnectFrame
}

// Encode will encode the frame and return its components.
func (c *Connect) Encode() (*api.BaseCommand, error) {
	// prepare connect command
	connect := &api.CommandConnect{}

	// set fields
	connect.ClientVersion = proto.String(c.ClientVersion)
	connect.AuthMethod = &authMethodNone
	connect.ProtocolVersion = &protocolVersion

	// set proxy url if available
	if c.ProxyTargetBrokerURL != "" {
		connect.ProxyToBrokerUrl = proto.String(c.ProxyTargetBrokerURL)
	}

	// prepare base command
	base := &api.BaseCommand{
		Type:    getType(api.BaseCommand_CONNECT),
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
func (c *Connected) Decode(bc *api.BaseCommand) error {
	// check protocol version
	if bc.Connected.GetProtocolVersion() != int32(api.ProtocolVersion_v13) {
		return fmt.Errorf("only protocol version 13 is supported")
	}

	// set fields
	c.ServerVersion = bc.Connected.GetServerVersion()

	return nil
}
