package message

import "context"

// Producer defines a generic message publisher.
type Producer interface {
	// Publish sends a message to a topic or queue.
	Publish(ctx context.Context, topic string, message []byte) error
}

// Consumer defines a generic message consumer.
type Consumer interface {
	// Start begins consuming messages from a topic or queue.
	// The handler function should process the message payload.
	Start(ctx context.Context, topic string, handler func([]byte) error) error
}
