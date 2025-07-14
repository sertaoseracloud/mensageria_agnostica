package message

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	sdkoracle "github.com/oracle/oci-go-sdk/v65/common"

	repoaws "github.com/example/mensageria_agnostica/aws"
	"github.com/example/mensageria_agnostica/azure"
	"github.com/example/mensageria_agnostica/oracle"
	"github.com/example/mensageria_agnostica/rabbitmq"
)

// Provider identifies a messaging technology.
type Provider string

const (
	// ProviderRabbitMQ uses the amqp protocol.
	ProviderRabbitMQ Provider = "rabbitmq"
	// ProviderAWS uses AWS SQS.
	ProviderAWS Provider = "aws"
	// ProviderAzure uses Azure Service Bus.
	ProviderAzure Provider = "azure"
	// ProviderOracle uses Oracle Queue Service.
	ProviderOracle Provider = "oracle"
)

// RabbitMQOptions configures a RabbitMQ producer or consumer.
type RabbitMQOptions struct {
	URL string
}

// AWSOptions configures an AWS SQS producer or consumer.
type AWSOptions struct {
	Config   sdkaws.Config
	QueueURL string
}

// AzureOptions configures an Azure Service Bus producer or consumer.
type AzureOptions struct {
	Client *azservicebus.Client
	Queue  string
}

// OracleOptions configures an Oracle Queue Service producer or consumer.
type OracleOptions struct {
	Provider sdkoracle.ConfigurationProvider
	QueueID  string
}

// NewProducer returns a Producer implementation based on the provider.
func NewProducer(provider Provider, opts any) (Producer, error) {
	switch provider {
	case ProviderRabbitMQ:
		o, ok := opts.(RabbitMQOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for rabbitmq")
		}
		return rabbitmq.NewProducer(o.URL)
	case ProviderAWS:
		o, ok := opts.(AWSOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for aws")
		}
		return repoaws.NewSQSProducer(o.Config, o.QueueURL), nil
	case ProviderAzure:
		o, ok := opts.(AzureOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for azure")
		}
		return azure.NewServiceBusProducer(o.Client, o.Queue)
	case ProviderOracle:
		o, ok := opts.(OracleOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for oracle")
		}
		return oracle.NewQueueProducer(o.Provider, o.QueueID)
	default:
		return nil, fmt.Errorf("unknown provider %s", provider)
	}
}

// NewConsumer returns a Consumer implementation based on the provider.
func NewConsumer(provider Provider, opts any) (Consumer, error) {
	switch provider {
	case ProviderRabbitMQ:
		o, ok := opts.(RabbitMQOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for rabbitmq")
		}
		return rabbitmq.NewConsumer(o.URL)
	case ProviderAWS:
		o, ok := opts.(AWSOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for aws")
		}
		return repoaws.NewSQSConsumer(o.Config, o.QueueURL), nil
	case ProviderAzure:
		o, ok := opts.(AzureOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for azure")
		}
		return azure.NewServiceBusConsumer(o.Client, o.Queue)
	case ProviderOracle:
		o, ok := opts.(OracleOptions)
		if !ok {
			return nil, fmt.Errorf("invalid options for oracle")
		}
		return oracle.NewQueueConsumer(o.Provider, o.QueueID)
	default:
		return nil, fmt.Errorf("unknown provider %s", provider)
	}
}
