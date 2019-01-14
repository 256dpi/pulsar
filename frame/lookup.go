package frame

import (
	"fmt"

	"github.com/256dpi/pulsar/api"

	"github.com/golang/protobuf/proto"
)

// Lookup performs a topic lookup request.
type Lookup struct {
	// The request id.
	RID uint64

	// The topic to lookup.
	Topic string

	// The authoritative flag.
	//
	// Should be initially set to false. When following a redirect response,
	// clients should pass the same value contained in the response.
	Authoritative bool

	// TODO: Support original principal and auth method?
}

// Type will return the frame type.
func (l *Lookup) Type() Type {
	return LookupFrame
}

// Encode will encode the frame and return its components.
func (l *Lookup) Encode() (*api.BaseCommand, error) {
	// prepare lookup command
	lookup := &api.CommandLookupTopic{}
	lookup.Topic = proto.String(l.Topic)
	lookup.RequestId = proto.Uint64(l.RID)
	lookup.Authoritative = proto.Bool(l.Authoritative)

	// prepare base command
	base := &api.BaseCommand{
		Type:        getType(api.BaseCommand_LOOKUP),
		LookupTopic: lookup,
	}

	return base, nil
}

// LookupResponseType defines the type of the lookup response.
type LookupResponseType int

const (
	// Redirect instructs the client to redirect the lookup request to the
	// provided broker.
	Redirect = LookupResponseType(api.CommandLookupTopicResponse_Redirect)

	// Final instructs to connect to the provided broker.
	Final = LookupResponseType(api.CommandLookupTopicResponse_Connect)

	// Failed defines a failed lookup request.
	Failed = LookupResponseType(api.CommandLookupTopicResponse_Failed)
)

// LookupResponse is received as a response to the Lookup request.
type LookupResponse struct {
	// The request id.
	RID uint64

	// The response type.
	ResponseType LookupResponseType

	// The URL of the broker serving the topic.
	BrokerURL string

	// The authoritative flag that should be forwarded when following a redirect.
	Authoritative bool

	// If set the client should proxy through the service URL.
	ProxyThroughServiceURL bool

	// The error if failed
	ErrorCode string

	// The error message if failed.
	ErrorMessage string

	// TODO: Support secure broker url.
}

// Type will return the frame type.
func (r *LookupResponse) Type() Type {
	return LookupResponseFrame
}

// Error implements the error interface.
func (r *LookupResponse) Error() string {
	if r.ErrorMessage != "" {
		return fmt.Sprintf("pulsar: %s: %s", r.ErrorCode, r.ErrorMessage)
	}

	return fmt.Sprintf("pulsar: %s", r.ErrorCode)
}

// Decode will construct the frame from the specified components.
func (r *LookupResponse) Decode(bc *api.BaseCommand) error {
	// set fields
	r.RID = bc.LookupTopicResponse.GetRequestId()
	r.ResponseType = LookupResponseType(bc.LookupTopicResponse.GetResponse())
	r.BrokerURL = bc.LookupTopicResponse.GetBrokerServiceUrl()
	r.Authoritative = bc.LookupTopicResponse.GetAuthoritative()
	r.ProxyThroughServiceURL = bc.LookupTopicResponse.GetProxyThroughServiceUrl()

	// read error info if failed
	if r.ResponseType == Failed {
		r.ErrorCode = api.ServerError_name[int32(bc.LookupTopicResponse.GetError())]
		r.ErrorMessage = bc.LookupTopicResponse.GetMessage()
	}

	return nil
}
