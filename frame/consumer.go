package frame

import "github.com/256dpi/pulsar/api"

// ActiveConsumerChange is sent by the broker to inform about consumer changes.
type ActiveConsumerChange struct {
	// The consumer id.
	CID uint64

	// If the consumer is active.
	Active bool
}

// Type will return the frame type.
func (c *ActiveConsumerChange) Type() Type {
	return ActiveConsumerChangeFrame
}

// Decode will construct the frame from the specified components.
func (c *ActiveConsumerChange) Decode(bc *api.BaseCommand) error {
	// set fields
	c.CID = bc.ActiveConsumerChange.GetConsumerId()
	c.Active = bc.ActiveConsumerChange.GetIsActive()

	return nil
}

// ReachedEndOfTopic is sent by the broker to inform about a topic termination.
type ReachedEndOfTopic struct {
	// The consumer id.
	CID uint64
}

// Type will return the frame type.
func (t *ReachedEndOfTopic) Type() Type {
	return ReachedEndOfTopicFrame
}

// Decode will construct the frame from the specified components.
func (t *ReachedEndOfTopic) Decode(bc *api.BaseCommand) error {
	// set fields
	t.CID = bc.ReachedEndOfTopic.GetConsumerId()

	return nil
}
