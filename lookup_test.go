package pulsar

import (
	"testing"

	"github.com/256dpi/pulsar/frame"

	"github.com/stretchr/testify/assert"
)

func TestLookup(t *testing.T) {
	res, redirects, err := Lookup("pulsar://0.0.0.0:6650", "test")
	assert.NoError(t, err)
	assert.Equal(t, 0, redirects)
	assert.Equal(t, &frame.LookupResponse{
		ResponseType:          frame.Final,
		BrokerURL:             "pulsar://Odin.local:6650",
		Authoritative:         true,
		ProxyThroughBrokerURL: true,
	}, res)
}
