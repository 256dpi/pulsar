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
	err = client.Lookup("test", false, func(res *LookupResponse, err error) {
		assert.NoError(t, err)
		assert.Equal(t, &LookupResponse{
			URL:           "pulsar://Odin.local:6650",
			Authoritative: true,
			Proxy:         true,
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
	})
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
