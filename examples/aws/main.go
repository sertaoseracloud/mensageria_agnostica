package main

import (
    "context"
    "log"
    "time"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/example/mensageria_agnostica/message"
)

func main() {
    ctx := context.Background()

    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        log.Fatalf("carregando config AWS: %v", err)
    }

    prod, err := message.NewProducer(
        message.ProviderAWS,
        message.AWSOptions{Config: cfg, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/poc"},
    )
    if err != nil {
        log.Fatalf("criando produtor: %v", err)
    }
    if err := prod.Publish(ctx, "", []byte("mensagem de teste")); err != nil {
        log.Fatalf("publicando mensagem: %v", err)
    }

    cons, err := message.NewConsumer(
        message.ProviderAWS,
        message.AWSOptions{Config: cfg, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/poc"},
    )
    if err != nil {
        log.Fatalf("criando consumidor: %v", err)
    }
    cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    cons.Start(cctx, "", func(msg []byte) error {
        log.Printf("recebido: %s", string(msg))
        return nil
    })
}
