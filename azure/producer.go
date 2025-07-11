package azure

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// ServiceBusProducer sends messages to Azure Service Bus.
type ServiceBusProducer struct {
	sender *azservicebus.Sender
}

// NewServiceBusProducer creates a new ServiceBusProducer.
func NewServiceBusProducer(client *azservicebus.Client, queue string) (*ServiceBusProducer, error) {
	sender, err := client.NewSender(queue, nil)
	if err != nil {
		return nil, err
	}
	return &ServiceBusProducer{sender: sender}, nil
}

// Publish sends a message to the Service Bus queue.
func (p *ServiceBusProducer) Publish(ctx context.Context, _ string, msg []byte) error {
	return p.sender.SendMessage(ctx, &azservicebus.Message{Body: msg}, nil)
}

// Close closes the sender.
func (p *ServiceBusProducer) Close(ctx context.Context) error {
	return p.sender.Close(ctx)
}
