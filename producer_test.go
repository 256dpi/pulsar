package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateProducer(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerConfig{
		Name:  "test",
		Topic: "test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	// TODO: Close Producer.

	err = client.Close()
	assert.NoError(t, err)
}
