package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookup(t *testing.T) {
	config, redirects, err := Lookup("pulsar://localhost:6650", "test")
	assert.NoError(t, err)
	assert.Equal(t, 0, redirects)
	assert.Equal(t, ClientConfig{
		PhysicalBrokerURL: "pulsar://localhost:6650",
		LogicalBrokerURL:  "pulsar://Odin.local:6650",
	}, config)
}
