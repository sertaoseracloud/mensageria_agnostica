package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
    "github.com/example/mensageria_agnostica/message"
)

func main() {
    ctx := context.Background()

    connStr := os.Getenv("AZURE_SERVICEBUS_CONNECTION")
    client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
    if err != nil {
        log.Fatalf("criando cliente azure: %v", err)
    }

    opts := message.AzureOptions{Client: client, Queue: "poc"}
    prod, err := message.NewProducer(message.ProviderAzure, opts)
    if err != nil {
        log.Fatalf("criando produtor: %v", err)
    }
    if err := prod.Publish(ctx, "poc", []byte("mensagem de teste")); err != nil {
        log.Fatalf("publicando mensagem: %v", err)
    }

    cons, err := message.NewConsumer(message.ProviderAzure, opts)
    if err != nil {
        log.Fatalf("criando consumidor: %v", err)
    }
    cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    cons.Start(cctx, "poc", func(msg []byte) error {
        log.Printf("recebido: %s", string(msg))
        return nil
    })
}
