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
