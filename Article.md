# Sistema de Mensageria Agnóstico em Go

Este artigo apresenta um passo a passo da implementação de um sistema de mensagens em Go que abstrai diferentes tecnologias de mensageria. O objetivo é permitir que o código da aplicação não dependa diretamente de RabbitMQ, AWS SQS, Azure Service Bus ou Oracle Queue Service. Para isso utilizamos um _factory pattern_ que cria produtores e consumidores de forma transparente. A seguir detalhamos cada parte do código e mostramos como executar os exemplos.

## Interfaces Genéricas

O primeiro passo foi definir interfaces que representam as operações mínimas de envio e consumo de mensagens. Elas residem no pacote `message`.

```go
// Producer define um publicador genérico.
type Producer interface {
    Publish(ctx context.Context, topic string, message []byte) error
}

// Consumer define um consumidor genérico.
type Consumer interface {
    Start(ctx context.Context, topic string, handler func([]byte) error) error
}
```

O arquivo `message/interfaces.go` completo fica assim:

```go
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
```

Essas interfaces isolam o restante do código das particularidades de cada provedor de mensageria.

### Estrutura do Projeto

```
mensageria_agnostica/
├── aws/        # Implementação para AWS SQS
├── azure/      # Implementação para Azure Service Bus
├── oracle/     # Implementação para Oracle Queue Service
├── rabbitmq/   # Implementação para RabbitMQ
├── examples/   # Programas demonstrando cada provedor
├── example/    # Exemplo com Docker Compose
└── message/    # Interfaces e fábrica
```

## Fábrica de Produtores e Consumidores

Para instanciar as implementações corretas é oferecida a função `NewProducer` e `NewConsumer`. Cada provedor possui um tipo de opções específico para configurar conexões e filas.

```go
func NewProducer(provider Provider, opts any) (Producer, error) {
    switch provider {
    case ProviderRabbitMQ:
        o := opts.(RabbitMQOptions)
        return rabbitmq.NewProducer(o.URL)
    case ProviderAWS:
        o := opts.(AWSOptions)
        return aws.NewSQSProducer(o.Config, o.QueueURL), nil
    case ProviderAzure:
        o := opts.(AzureOptions)
        return azure.NewServiceBusProducer(o.Client, o.Queue)
    case ProviderOracle:
        o := opts.(OracleOptions)
        return oracle.NewQueueProducer(o.Provider, o.QueueID)
    default:
        return nil, fmt.Errorf("unknown provider %s", provider)
    }
}
```

O arquivo `message/factory.go` traz a definição completa da função acima e também de `NewConsumer`:

```go
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

type Provider string

const (
    ProviderRabbitMQ Provider = "rabbitmq"
    ProviderAWS      Provider = "aws"
    ProviderAzure    Provider = "azure"
    ProviderOracle   Provider = "oracle"
)

type RabbitMQOptions struct {
    URL string
}

type AWSOptions struct {
    Config   sdkaws.Config
    QueueURL string
}

type AzureOptions struct {
    Client *azservicebus.Client
    Queue  string
}

type OracleOptions struct {
    Provider sdkoracle.ConfigurationProvider
    QueueID  string
}

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
```

A mesma lógica é aplicada para consumidores. Dessa forma é possível trocar de tecnologia apenas mudando o `Provider` e as opções de criação.

## Implementações Específicas

Cada provedor possui seu pacote dedicado:

- `rabbitmq` utiliza a biblioteca `amqp091-go`. O produtor estabelece a conexão e envia mensagens via `PublishWithContext`. O consumidor registra um `handler` e consome mensagens com `ConsumeWithContext`.
- `aws` implementa comunicação com SQS usando `aws-sdk-go-v2`. As mensagens são codificadas em Base64 para compatibilidade.
- `azure` usa `azservicebus`. O produtor cria um `Sender` enquanto o consumidor cria um `Receiver` para ler as mensagens.
- `oracle` utiliza o SDK `oci-go-sdk`. As mensagens são manipuladas por meio do `queue.QueueClient`.

Cada implementação adere às interfaces genéricas, permitindo uso intercambiável.

### Código completo por provedor

A seguir listamos, para fins de estudo, a implementação integral dos produtores e consumidores de cada serviço.

#### RabbitMQ

```go
// rabbitmq/producer.go
package rabbitmq

import (
    "context"

    amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
    conn    *amqp.Connection
    channel *amqp.Channel
}

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

func (p *Producer) Publish(ctx context.Context, queue string, msg []byte) error {
    return p.channel.PublishWithContext(ctx, "", queue, false, false,
        amqp.Publishing{ContentType: "application/octet-stream", Body: msg})
}

func (p *Producer) Close() error {
    if err := p.channel.Close(); err != nil {
        p.conn.Close()
        return err
    }
    return p.conn.Close()
}

// rabbitmq/consumer.go
package rabbitmq

import (
    "context"

    amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
    conn    *amqp.Connection
    channel *amqp.Channel
}

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

func (c *Consumer) Start(ctx context.Context, queue string, handler func([]byte) error) error {
    msgs, err := c.channel.ConsumeWithContext(ctx, queue, "", true, false, false, false, nil)
    if err != nil {
        return err
    }
    for m := range msgs {
        _ = handler(m.Body)
    }
    return ctx.Err()
}

func (c *Consumer) Close() error {
    if err := c.channel.Close(); err != nil {
        c.conn.Close()
        return err
    }
    return c.conn.Close()
}
```

#### AWS SQS

```go
// aws/producer.go
package aws

import (
    "context"
    "encoding/base64"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSProducer struct {
    client   *sqs.Client
    queueURL string
}

func NewSQSProducer(cfg aws.Config, queueURL string) *SQSProducer {
    return &SQSProducer{
        client:   sqs.NewFromConfig(cfg),
        queueURL: queueURL,
    }
}

func (p *SQSProducer) Publish(ctx context.Context, _ string, msg []byte) error {
    _, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
        QueueUrl:    &p.queueURL,
        MessageBody: aws.String(base64.StdEncoding.EncodeToString(msg)),
    })
    return err
}

// aws/consumer.go
package aws

import (
    "context"
    "encoding/base64"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSConsumer struct {
    client   *sqs.Client
    queueURL string
}

func NewSQSConsumer(cfg aws.Config, queueURL string) *SQSConsumer {
    return &SQSConsumer{
        client:   sqs.NewFromConfig(cfg),
        queueURL: queueURL,
    }
}

func (c *SQSConsumer) Start(ctx context.Context, _ string, handler func([]byte) error) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
            QueueUrl:            &c.queueURL,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds:     20,
        })
        if err != nil {
            return err
        }
        if len(out.Messages) == 0 {
            continue
        }
        for _, m := range out.Messages {
            body, err := base64.StdEncoding.DecodeString(aws.ToString(m.Body))
            if err != nil {
                return err
            }
            if err := handler(body); err != nil {
                return err
            }
            _, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
                QueueUrl:      &c.queueURL,
                ReceiptHandle: m.ReceiptHandle,
            })
            if err != nil {
                return err
            }
        }
    }
}
```

#### Azure Service Bus

```go
// azure/producer.go
package azure

import (
    "context"

    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type ServiceBusProducer struct {
    sender *azservicebus.Sender
}

func NewServiceBusProducer(client *azservicebus.Client, queue string) (*ServiceBusProducer, error) {
    sender, err := client.NewSender(queue, nil)
    if err != nil {
        return nil, err
    }
    return &ServiceBusProducer{sender: sender}, nil
}

func (p *ServiceBusProducer) Publish(ctx context.Context, _ string, msg []byte) error {
    return p.sender.SendMessage(ctx, &azservicebus.Message{Body: msg}, nil)
}

func (p *ServiceBusProducer) Close(ctx context.Context) error {
    return p.sender.Close(ctx)
}

// azure/consumer.go
package azure

import (
    "context"
    "errors"

    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type ServiceBusConsumer struct {
    receiver *azservicebus.Receiver
}

func NewServiceBusConsumer(client *azservicebus.Client, queue string) (*ServiceBusConsumer, error) {
    receiver, err := client.NewReceiverForQueue(queue, nil)
    if err != nil {
        return nil, err
    }
    return &ServiceBusConsumer{receiver: receiver}, nil
}

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

func (c *ServiceBusConsumer) Close(ctx context.Context) error {
    return c.receiver.Close(ctx)
}
```

#### Oracle Queue Service

```go
// oracle/producer.go
package oracle

import (
    "context"

    "github.com/oracle/oci-go-sdk/v65/common"
    "github.com/oracle/oci-go-sdk/v65/queue"
)

type QueueProducer struct {
    client  *queue.QueueClient
    queueID string
}

func NewQueueProducer(cfg common.ConfigurationProvider, queueID string) (*QueueProducer, error) {
    client, err := queue.NewQueueClientWithConfigurationProvider(cfg)
    if err != nil {
        return nil, err
    }
    return &QueueProducer{client: &client, queueID: queueID}, nil
}

func (p *QueueProducer) Publish(ctx context.Context, _ string, msg []byte) error {
    _, err := p.client.PutMessages(ctx, queue.PutMessagesRequest{
        QueueId: &p.queueID,
        PutMessagesDetails: queue.PutMessagesDetails{
            Messages: []queue.PutMessagesDetailsEntry{{Content: common.String(string(msg))}},
        },
    })
    return err
}

// oracle/consumer.go
package oracle

import (
    "context"

    "github.com/oracle/oci-go-sdk/v65/common"
    "github.com/oracle/oci-go-sdk/v65/queue"
)

type QueueConsumer struct {
    client  *queue.QueueClient
    queueID string
}

func NewQueueConsumer(cfg common.ConfigurationProvider, queueID string) (*QueueConsumer, error) {
    client, err := queue.NewQueueClientWithConfigurationProvider(cfg)
    if err != nil {
        return nil, err
    }
    return &QueueConsumer{client: &client, queueID: queueID}, nil
}

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
```

## Exemplo Prático e Containers

No diretório `example` há um `docker-compose.yml` que sobe uma instância do RabbitMQ e executa um programa de demonstração. O `Dockerfile` faz um _build_ multi-stage e gera uma imagem enxuta baseada no distroless.

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
  app:
    build:
      context: ..
      dockerfile: example/Dockerfile
    depends_on:
      - rabbitmq
```

O código de exemplo publica uma mensagem na fila `poc` e inicia um consumidor que exibe a mensagem recebida.

```go
prod, _ := message.NewProducer(
    message.ProviderRabbitMQ,
    message.RabbitMQOptions{URL: "amqp://guest:guest@rabbitmq:5672/"},
)
prod.Publish(ctx, "poc", []byte("mensagem de teste"))
```

É possível repetir o mesmo fluxo com os programas em `examples/aws`, `examples/azure` e `examples/oracle`, bastando fornecer as credenciais apropriadas.

### Exemplo com AWS SQS

O diretório `examples/aws` traz um programa simples que publica e consome uma mensagem da fila configurada na AWS. Para executar o exemplo utilize:

```bash
go run ./examples/aws
```

### Exemplo com Azure Service Bus

Para Azure, defina a variável `AZURE_SERVICEBUS_CONNECTION` com a connection string do Service Bus e rode o programa em `examples/azure`:

```bash
AZURE_SERVICEBUS_CONNECTION="Endpoint=sb://..." go run ./examples/azure
```

## Conclusão

A combinação das interfaces genéricas com o _factory pattern_ torna a solução flexível e facilmente extensível. Novos provedores podem ser adicionados apenas implementando as interfaces e registrando-os na fábrica. Essa abordagem simplifica testes e permite que a aplicação foque na lógica de negócio, deixando a escolha da tecnologia de mensageria em segundo plano.
