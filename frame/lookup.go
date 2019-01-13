package frame

import (
	"github.com/256dpi/pulsar/pb"

	"github.com/golang/protobuf/proto"
)

type Lookup struct {
	RID           uint64
	Topic         string
	Authoritative bool
}

func (l *Lookup) Encode() (*pb.BaseCommand, error) {
	// prepare lookup command
	lookup := &pb.CommandLookupTopic{}
	lookup.Topic = proto.String(l.Topic)
	lookup.RequestId = proto.Uint64(l.RID)
	lookup.Authoritative = proto.Bool(l.Authoritative)

	// prepare base command
	base := &pb.BaseCommand{
		Type:        getType(pb.BaseCommand_LOOKUP),
		LookupTopic: lookup,
	}

	return base, nil
}

type LookupType int

const (
	LookupTypeRedirect = LookupType(pb.CommandLookupTopicResponse_Redirect)
	LookupTypeConnect  = LookupType(pb.CommandLookupTopicResponse_Connect)
	LookupTypeFailed   = LookupType(pb.CommandLookupTopicResponse_Failed)
)

type LookupResponse struct {
	RID                    uint64
	BrokerServiceURL       string
	BrokerServiceURLTLS    string
	Response               LookupType
	Authoritative          bool
	Error                  string
	Message                string
	ProxyThroughServiceURL bool
}

func (r *LookupResponse) Decode(bc *pb.BaseCommand) error {
	// set fields
	r.RID = bc.LookupTopicResponse.GetRequestId()
	r.BrokerServiceURL = bc.LookupTopicResponse.GetBrokerServiceUrl()
	r.BrokerServiceURLTLS = bc.LookupTopicResponse.GetBrokerServiceUrlTls()
	r.Response = LookupType(bc.LookupTopicResponse.GetResponse())
	r.Authoritative = bc.LookupTopicResponse.GetAuthoritative()
	r.Error = pb.ServerError_name[int32(bc.LookupTopicResponse.GetError())]
	r.Message = bc.LookupTopicResponse.GetMessage()
	r.ProxyThroughServiceURL = bc.LookupTopicResponse.GetProxyThroughServiceUrl()

	return nil
}
