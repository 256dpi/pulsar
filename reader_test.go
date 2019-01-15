package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	queue := make(chan interface{}, 1)

	reader, err := CreateReader(ReaderConfig{
		Name:             "test11",
		Topic:            "public/test/test11",
		Subscription:     "test11",
		InflightMessages: 1,
		MessageCallback: func(msg ReaderMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	sendMessage("public/test/test11", []byte("test11"))

	msg := safeReceive(queue).(ReaderMessage)
	assert.Equal(t, []byte("test11"), msg.Payload)

	err = reader.Close()
	assert.NoError(t, err)
}
