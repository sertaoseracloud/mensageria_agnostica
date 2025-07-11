package aws

import (
	"context"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSConsumer receives messages from AWS SQS queues.
type SQSConsumer struct {
	client   *sqs.Client
	queueURL string
}

// NewSQSConsumer creates a new SQSConsumer.
func NewSQSConsumer(cfg aws.Config, queueURL string) *SQSConsumer {
	return &SQSConsumer{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}
}

// Start begins polling messages from the queue.
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
