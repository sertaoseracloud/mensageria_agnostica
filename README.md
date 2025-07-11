# Mensageria Agnóstica

Este repositório contém uma implementação simples de produtores e consumidores de mensagens em Go, utilizando uma interface genérica para ser agnóstico à tecnologia de mensageria. Há exemplos de implementação para:

- **RabbitMQ**
- **AWS SQS**
- **Azure Service Bus**
- **Oracle Queue Service**

As interfaces genéricas estão no pacote `message` e permitem abstrair o envio e o recebimento de mensagens independente do provedor utilizado.

```go
// Exemplo de uso da fábrica de produtores
prod, _ := message.NewProducer(
    message.ProviderRabbitMQ,
    message.RabbitMQOptions{URL: "amqp://guest:guest@localhost:5672/"},
)
err := prod.Publish(context.Background(), "fila", []byte("exemplo"))
```

Cada pacote possui implementações de `Producer` e `Consumer` para o respectivo serviço de mensageria.

## Execução dos testes

Execute `go vet ./...` e `go test ./...` para validar o código.

## Exemplo em Container

Para executar uma prova de conceito utilizando RabbitMQ, é fornecido um `docker-compose.yml` em `example/`. Basta possuir o Docker instalado e executar:

```bash
cd example
docker compose up --build
```

O serviço `app` irá publicar e consumir uma mensagem da fila `poc`, enquanto o contêiner `rabbitmq` disponibiliza uma instância do broker para testes.

## Exemplos por Provedor

O diretório `examples` traz programas que enviam e consomem uma mensagem da fila `poc` para cada provedor suportado.

### RabbitMQ
```go
prod, _ := message.NewProducer(
    message.ProviderRabbitMQ,
    message.RabbitMQOptions{URL: "amqp://guest:guest@localhost:5672/"},
)
```

### AWS SQS
```go
cfg, _ := config.LoadDefaultConfig(context.Background())
prod, _ := message.NewProducer(
    message.ProviderAWS,
    message.AWSOptions{Config: cfg, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/poc"},
)
```

### Azure Service Bus
```go
client, _ := azservicebus.NewClientFromConnectionString(connStr, nil)
prod, _ := message.NewProducer(
    message.ProviderAzure,
    message.AzureOptions{Client: client, Queue: "poc"},
)
```

### Oracle Queue Service
```go
provider := common.DefaultConfigProvider()
prod, _ := message.NewProducer(
    message.ProviderOracle,
    message.OracleOptions{Provider: provider, QueueID: "ocid1.queue.oc1..."},
)
```
