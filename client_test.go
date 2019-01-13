package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientConnection(t *testing.T) {
	c := NewClient(ClientConfig{})

	err := c.Connect()
	assert.NoError(t, err)

	err = c.Close()
	assert.NoError(t, err)
}
