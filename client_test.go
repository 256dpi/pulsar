package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestLookup(t *testing.T) {
	/* first lookup */

	client1, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	resp1, err := client1.Lookup("test", false)
	assert.NoError(t, err)
	assert.Equal(t, &LookupResponse{
		URL:           "pulsar://Odin.local:6650",
		Authoritative: true,
		Proxy:         true,
	}, resp1)

	err = client1.Close()
	assert.NoError(t, err)

	/* second lookup */

	cfg := ClientConfig{
		URL: resp1.URL,
	}

	if resp1.Proxy {
		cfg.ProxyURL = resp1.URL
	}

	client2, err := Connect(cfg)
	assert.NoError(t, err)

	resp2, err := client2.Lookup("test", resp1.Authoritative)
	assert.NoError(t, err)
	assert.Equal(t, &LookupResponse{
		URL:           "pulsar://Odin.local:6650",
		Authoritative: true,
		Proxy:         true,
	}, resp2)

	err = client2.Close()
	assert.NoError(t, err)
}

func TestCreateProducer(t *testing.T) {
	client, err := Connect(ClientConfig{})
	assert.NoError(t, err)

	id, lastSeq, err := client.CreateProducer("test", "test")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), id)
	assert.Equal(t, int64(-1), lastSeq)

	var seq uint64 = 0
	if lastSeq > 0 {
		seq = uint64(lastSeq + 1)
	}

	err = client.Send(id, seq, []byte("hello"))
	assert.NoError(t, err)

	err = client.CloseProducer(id)
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}
