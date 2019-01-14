package pulsar

import (
	"errors"
	"time"

	"github.com/256dpi/pulsar/frame"
)

// DefaultTimeout is the default timeout used if not further specified.
const DefaultTimeout = 10 * time.Second

// ErrTimeout is returned if operations failed due to a timeout.
var ErrTimeout = errors.New("timeout")

// MessageID is the id of a single message.
type MessageID = frame.MessageID
