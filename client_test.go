package pulsar

import (
	"testing"

	"github.com/256dpi/pulsar/frame"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestLookup(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	done := make(chan struct{})
	err = client.Lookup("test", false, func(res *frame.LookupResponse, err error) {
		assert.NoError(t, err)
		assert.Equal(t, &frame.LookupResponse{
			BrokerURL:             "pulsar://Odin.local:6650",
			ResponseType:          frame.Final,
			Authoritative:         true,
			ProxyThroughBrokerURL: true,
		}, res)

		close(done)
	})
	assert.NoError(t, err)

	safeWait(done)

	err = client.Close()
	assert.NoError(t, err)
}

func TestCreateProducer(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	var pid uint64 = 0
	var seq uint64 = 0
	done1 := make(chan struct{})
	err = client.CreateProducer("test", "test", func(id uint64, lastSeq int64, err error) {
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), id)
		assert.Equal(t, int64(-1), lastSeq)

		pid = id
		if lastSeq > 0 {
			seq = uint64(lastSeq + 1)
		}

		close(done1)
	})
	assert.NoError(t, err)

	safeWait(done1)

	done2 := make(chan struct{})
	err = client.Send(pid, seq, []byte("hello"), func(e error) {
		assert.NoError(t, err)

		close(done2)
	})
	assert.NoError(t, err)

	safeWait(done2)

	done3 := make(chan struct{})
	err = client.CloseProducer(pid, func(e error) {
		assert.NoError(t, err)

		close(done3)
	})
	assert.NoError(t, err)

	safeWait(done3)

	err = client.Close()
	assert.NoError(t, err)
}

func TestCreateConsumer(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	var cid uint64
	done1 := make(chan struct{})
	err = client.CreateConsumer("test", "test", "test", frame.Exclusive, false, func(id uint64, err error) {
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), cid)

		cid = id

		close(done1)
	}, nil)
	assert.NoError(t, err)

	safeWait(done1)

	done2 := make(chan struct{})
	err = client.CloseConsumer(cid, func(err error) {
		assert.NoError(t, err)

		close(done2)
	})
	assert.NoError(t, err)

	safeWait(done2)

	err = client.Close()
	assert.NoError(t, err)
}

func TestConsumerAndProducer(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	var pid uint64 = 0
	var seq uint64 = 0

	done1 := make(chan struct{})
	err = client.CreateProducer("test", "test", func(id uint64, lastSeq int64, err error) {
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), id)
		assert.Equal(t, int64(-1), lastSeq)

		pid = id
		if lastSeq > 0 {
			seq = uint64(lastSeq + 1)
		}

		close(done1)
	})
	assert.NoError(t, err)

	safeWait(done1)

	var cid uint64
	var mid frame.MessageID
	done2 := make(chan struct{})
	done4 := make(chan struct{})
	err = client.CreateConsumer("test", "test", "test", frame.Exclusive, false, func(id uint64, err error) {
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), cid)

		cid = id

		close(done2)
	}, func(msg *frame.Message, err error) {
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello"), msg.Payload)

		mid = msg.MessageID
		close(done4)
	})
	assert.NoError(t, err)

	err = client.Flow(cid, 10)
	assert.NoError(t, err)

	safeWait(done2)

	done3 := make(chan struct{})
	err = client.Send(pid, seq, []byte("hello"), func(e error) {
		assert.NoError(t, err)

		close(done3)
	})
	assert.NoError(t, err)

	safeWait(done3)
	safeWait(done4)

	err = client.Ack(cid, frame.Cumulative, mid)
	assert.NoError(t, err)

	done5 := make(chan struct{})
	err = client.CloseConsumer(cid, func(err error) {
		assert.NoError(t, err)

		close(done5)
	})
	assert.NoError(t, err)

	safeWait(done5)

	done6 := make(chan struct{})
	err = client.CloseProducer(pid, func(e error) {
		assert.NoError(t, err)

		close(done6)
	})
	assert.NoError(t, err)

	safeWait(done6)

	err = client.Close()
	assert.NoError(t, err)
}
