package azure

import (
	"context"
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// ServiceBusConsumer receives messages from Azure Service Bus.
type ServiceBusConsumer struct {
	receiver *azservicebus.Receiver
}

// NewServiceBusConsumer creates a new ServiceBusConsumer.
func NewServiceBusConsumer(client *azservicebus.Client, queue string) (*ServiceBusConsumer, error) {
	receiver, err := client.NewReceiverForQueue(queue, nil)
	if err != nil {
		return nil, err
	}
	return &ServiceBusConsumer{receiver: receiver}, nil
}

// Start receives messages and invokes the handler for each one.
func (c *ServiceBusConsumer) Start(ctx context.Context, _ string, handler func([]byte) error) error {
	for {
		msgs, err := c.receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			var sbErr *azservicebus.Error
			if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout {
				continue
			}
			return err
		}
		for _, msg := range msgs {
			if err := handler(msg.Body); err != nil {
				return err
			}
			if err := c.receiver.CompleteMessage(ctx, msg, nil); err != nil {
				return err
			}
		}
	}
}

// Close closes the receiver.
func (c *ServiceBusConsumer) Close(ctx context.Context) error {
	return c.receiver.Close(ctx)
}
