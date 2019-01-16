package pulsar

import (
	"errors"
	"github.com/256dpi/pulsar/frame"
)

// ErrTimeout is returned if operations failed due to a timeout.
var ErrTimeout = errors.New("timeout")

// MessageID is the id of a single message.
type MessageID = frame.MessageID
