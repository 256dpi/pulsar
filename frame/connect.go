package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/pb"
	"github.com/golang/protobuf/proto"
)

var authMethodNone = pb.AuthMethod_AuthMethodNone
var protocolVersion = int32(pb.ProtocolVersion_v13)

type Connect struct {
	ClientVersion string
}

func (c *Connect) Encode() (*pb.BaseCommand, error) {
	// prepare connect command
	connect := &pb.CommandConnect{}
	connect.ClientVersion = proto.String(c.ClientVersion)
	connect.AuthMethod = &authMethodNone
	connect.ProtocolVersion = &protocolVersion

	// prepare base command
	base := &pb.BaseCommand{
		Type:    getType(pb.BaseCommand_CONNECT),
		Connect: connect,
	}

	return base, nil
}

type Connected struct {
	ServerVersion string
}

func (c *Connected) Decode(bc *pb.BaseCommand) error {
	// check protocol version
	if bc.Connected.GetProtocolVersion() != int32(pb.ProtocolVersion_v13) {
		return fmt.Errorf("only protocol version 13 is supported")
	}

	// set fields
	c.ServerVersion = bc.Connected.GetServerVersion()

	return nil
}
