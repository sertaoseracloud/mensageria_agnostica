package oracle

import (
	"context"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/queue"
)

// QueueProducer publishes messages to Oracle Queue Service.
type QueueProducer struct {
	client  *queue.QueueClient
	queueID string
}

// NewQueueProducer creates a new QueueProducer.
func NewQueueProducer(cfg common.ConfigurationProvider, queueID string) (*QueueProducer, error) {
	client, err := queue.NewQueueClientWithConfigurationProvider(cfg)
	if err != nil {
		return nil, err
	}
	return &QueueProducer{client: &client, queueID: queueID}, nil
}

// Publish sends a message to the queue.
func (p *QueueProducer) Publish(ctx context.Context, _ string, msg []byte) error {
	_, err := p.client.PutMessages(ctx, queue.PutMessagesRequest{
		QueueId: &p.queueID,
		PutMessagesDetails: queue.PutMessagesDetails{
			Messages: []queue.PutMessagesDetailsEntry{
				{Content: common.String(string(msg))},
			},
		},
	})
	return err
}
