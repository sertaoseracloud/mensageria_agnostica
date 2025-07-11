# Sistema de Mensageria Agnóstico em Go

Este artigo apresenta um passo a passo da implementação de um sistema de mensagens em Go que abstrai diferentes tecnologias de mensageria. O objetivo é permitir que o código da aplicação não dependa diretamente de RabbitMQ, AWS SQS, Azure Service Bus ou Oracle Queue Service. Para isso utilizamos um _factory pattern_ que cria produtores e consumidores de forma transparente.

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

Essas interfaces isolam o restante do código das particularidades de cada provedor de mensageria.

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

A mesma lógica é aplicada para consumidores. Dessa forma é possível trocar de tecnologia apenas mudando o `Provider` e as opções de criação.

## Implementações Específicas

Cada provedor possui seu pacote dedicado:

- `rabbitmq` utiliza a biblioteca `amqp091-go`. O produtor estabelece a conexão e envia mensagens via `PublishWithContext`. O consumidor registra um `handler` e consome mensagens com `ConsumeWithContext`.
- `aws` implementa comunicação com SQS usando `aws-sdk-go-v2`. As mensagens são codificadas em Base64 para compatibilidade.
- `azure` usa `azservicebus`. O produtor cria um `Sender` enquanto o consumidor cria um `Receiver` para ler as mensagens.
- `oracle` utiliza o SDK `oci-go-sdk`. As mensagens são manipuladas por meio do `queue.QueueClient`.

Cada implementação adere às interfaces genéricas, permitindo uso intercambiável.

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

## Conclusão

A combinação das interfaces genéricas com o _factory pattern_ torna a solução flexível e facilmente extensível. Novos provedores podem ser adicionados apenas implementando as interfaces e registrando-os na fábrica. Essa abordagem simplifica testes e permite que a aplicação foque na lógica de negócio, deixando a escolha da tecnologia de mensageria em segundo plano.
