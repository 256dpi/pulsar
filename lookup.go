package pulsar

import (
	"errors"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"

	"github.com/patrickmn/go-cache"
)

// LookupResponseTimeout defines the time after a single lookup request is
// considered an error.
const LookupResponseTimeout = 10 * time.Second

// LookupRedirectLimit defines how many lookup redirects are followed before
// an error is triggered.
const LookupRedirectLimit = 5

const lookupCacheTimeout = time.Minute

// ErrLookupResponseTimeout is returned if the lookup response timeout has been
// reached.
var ErrLookupResponseTimeout = errors.New("lookup response timeout")

// ErrLookupRedirectLimit is returned if the redirect limit has been reached.
var ErrLookupRedirectLimit = errors.New("lookup redirect limit")

var lookupMutex sync.Mutex
var lookupClientCache = cache.New(lookupCacheTimeout, lookupCacheTimeout)

// Lookup will lookup the provided topic by sending and initial request to the
// specified broker service URL. Redirects are followed until a final response
// has been received.
func Lookup(serviceURL, topic string) (ClientConfig, int, error) {
	lookupMutex.Lock()
	defer lookupMutex.Unlock()

	// prepare config
	config := ClientConfig{}

	// perform initial lookup
	lr, err := singleLookup(serviceURL, topic, false)
	if err != nil {
		return config, 0, err
	}

	// prepare redirect counter
	redirects := 0

	// redirect until final response
	for lr.ResponseType == frame.Redirect {
		// check counter
		if redirects >= LookupRedirectLimit {
			return config, redirects, ErrLookupRedirectLimit
		}

		// perform lookup
		lr, err = singleLookup(lr.BrokerURL, topic, lr.Authoritative)
		if err != nil {
			return config, redirects, err
		}

		// increment
		redirects++
	}

	// check if proxy is requested
	if lr.ProxyThroughServiceURL {
		config.PhysicalBrokerURL = serviceURL
		config.LogicalBrokerURL = lr.BrokerURL
	} else {
		config.PhysicalBrokerURL = lr.BrokerURL
	}

	return config, redirects, nil
}

func singleLookup(url, topic string, authoritative bool) (*frame.LookupResponse, error) {
	// get cached client
	value, _ := lookupClientCache.Get(url)

	// coerce value
	client, ok := value.(*Client)

	// prepare config
	config := ClientConfig{
		PhysicalBrokerURL: url,
	}

	// create new client if not available
	if !ok {
		// create a new client
		newClient, err := Connect(config)
		if err != nil {
			return nil, err
		}

		// set client
		client = newClient
	}

	// perform initial lookup
	res, err := performLookup(client, topic, authoritative)
	if lookupErr, ok := err.(*frame.LookupResponse); ok {
		return nil, lookupErr
	} else if err != nil {
		// close and delete client in case of network level error
		_ = client.Close()
		lookupClientCache.Delete(url)

		return nil, err
	}

	// store client
	lookupClientCache.Set(url, client, lookupCacheTimeout)

	return res, nil
}

func performLookup(client *Client, topic string, authoritative bool) (*frame.LookupResponse, error) {
	// prepare channels
	responses := make(chan *frame.LookupResponse, 1)
	errs := make(chan error, 1)

	// perform lookup
	err := client.Lookup(topic, authoritative, func(res *frame.LookupResponse, err error) {
		// check error
		if err != nil {
			select {
			case errs <- err:
			default:
			}

			return
		}

		// send response
		select {
		case responses <- res:
		}
	})
	if err != nil {
		return nil, err
	}

	// await response
	select {
	case res := <-responses:
		return res, nil
	case err := <-errs:
		return nil, err
	case <-time.After(LookupResponseTimeout):
		return nil, ErrLookupResponseTimeout
	}
}
