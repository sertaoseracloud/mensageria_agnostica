package oracle

import (
	"context"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/queue"
)

// QueueConsumer receives messages from Oracle Queue Service.
type QueueConsumer struct {
	client  *queue.QueueClient
	queueID string
}

// NewQueueConsumer creates a new QueueConsumer.
func NewQueueConsumer(cfg common.ConfigurationProvider, queueID string) (*QueueConsumer, error) {
	client, err := queue.NewQueueClientWithConfigurationProvider(cfg)
	if err != nil {
		return nil, err
	}
	return &QueueConsumer{client: &client, queueID: queueID}, nil
}

// Start polls messages from the queue.
func (c *QueueConsumer) Start(ctx context.Context, _ string, handler func([]byte) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		out, err := c.client.GetMessages(ctx, queue.GetMessagesRequest{QueueId: &c.queueID})
		if err != nil {
			return err
		}
		if len(out.Messages) == 0 {
			continue
		}
		for _, m := range out.Messages {
			if err := handler([]byte(*m.Content)); err != nil {
				return err
			}
			_, err := c.client.DeleteMessage(ctx, queue.DeleteMessageRequest{QueueId: &c.queueID, MessageReceipt: m.Receipt})
			if err != nil {
				return err
			}
		}
	}
}
