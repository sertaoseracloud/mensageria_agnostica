package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer reads messages from RabbitMQ queues.
type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewConsumer establishes a connection and returns a new Consumer.
func NewConsumer(url string) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Consumer{conn: conn, channel: ch}, nil
}

// Start begins consuming messages from the queue.
func (c *Consumer) Start(ctx context.Context, queue string, handler func([]byte) error) error {
	msgs, err := c.channel.ConsumeWithContext(ctx, queue, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	for m := range msgs {
		if err := handler(m.Body); err != nil {
			// optional: send to dead letter queue or log
		}
	}
	return ctx.Err()
}

// Close releases channel and connection resources.
func (c *Consumer) Close() error {
	if err := c.channel.Close(); err != nil {
		c.conn.Close()
		return err
	}
	return c.conn.Close()
}
