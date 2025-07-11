package main

import (
    "context"
    "log"
    "time"

    "github.com/oracle/oci-go-sdk/v65/common"
    "github.com/example/mensageria_agnostica/message"
)

func main() {
    ctx := context.Background()

    provider := common.DefaultConfigProvider()

    opts := message.OracleOptions{Provider: provider, QueueID: "ocid1.queue.oc1..."}
    prod, err := message.NewProducer(message.ProviderOracle, opts)
    if err != nil {
        log.Fatalf("criando produtor: %v", err)
    }
    if err := prod.Publish(ctx, "poc", []byte("mensagem de teste")); err != nil {
        log.Fatalf("publicando mensagem: %v", err)
    }

    cons, err := message.NewConsumer(message.ProviderOracle, opts)
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
