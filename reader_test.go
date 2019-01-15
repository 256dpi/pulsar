package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	queue := make(chan interface{}, 3)

	reader1, err := CreateReader(ReaderConfig{
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

	sendMessage("public/test/test11", []byte("test11-1"))

	msg1 := safeReceive(queue).(ReaderMessage)
	assert.Equal(t, []byte("test11-1"), msg1.Payload)

	err = reader1.Close()
	assert.NoError(t, err)

	sendMessage("public/test/test11", []byte("test11-2"))
	sendMessage("public/test/test11", []byte("test11-3"))
	sendMessage("public/test/test11", []byte("test11-4"))

	reader2, err := CreateReader(ReaderConfig{
		Name:             "test11",
		Topic:            "public/test/test11",
		Subscription:     "test11",
		InflightMessages: 3,
		StartMessageID:   &msg1.ID,
		MessageCallback: func(msg ReaderMessage) {
			queue <- msg
		},
		ErrorCallback: func(err error) {
			assert.NoError(t, err)
		},
	})
	assert.NoError(t, err)

	msg2 := safeReceive(queue).(ReaderMessage)
	assert.Equal(t, []byte("test11-2"), msg2.Payload)

	msg3 := safeReceive(queue).(ReaderMessage)
	assert.Equal(t, []byte("test11-3"), msg3.Payload)

	msg4 := safeReceive(queue).(ReaderMessage)
	assert.Equal(t, []byte("test11-4"), msg4.Payload)

	err = reader2.Close()
	assert.NoError(t, err)
}
