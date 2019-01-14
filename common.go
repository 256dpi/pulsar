package pulsar

import (
	"errors"
	"time"
)

// DefaultTimeout is the default timeout used if not further specified.
const DefaultTimeout = 10 * time.Second

// ErrTimeout is returned if operations failed due to a timeout.
var ErrTimeout = errors.New("timeout")
