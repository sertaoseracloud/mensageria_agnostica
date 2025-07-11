package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer publishes messages to RabbitMQ queues.
type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewProducer establishes a connection and returns a new Producer.
func NewProducer(url string) (*Producer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Producer{conn: conn, channel: ch}, nil
}

// Publish sends a message to the given queue.
func (p *Producer) Publish(ctx context.Context, queue string, msg []byte) error {
	return p.channel.PublishWithContext(ctx, "", queue, false, false,
		amqp.Publishing{ContentType: "application/octet-stream", Body: msg})
}

// Close releases channel and connection resources.
func (p *Producer) Close() error {
	if err := p.channel.Close(); err != nil {
		p.conn.Close()
		return err
	}
	return p.conn.Close()
}
