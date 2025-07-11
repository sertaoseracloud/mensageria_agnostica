package aws

import (
	"context"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSProducer sends messages to AWS SQS queues.
type SQSProducer struct {
	client   *sqs.Client
	queueURL string
}

// NewSQSProducer creates a new SQSProducer.
func NewSQSProducer(cfg aws.Config, queueURL string) *SQSProducer {
	return &SQSProducer{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}
}

// Publish sends a message to the SQS queue.
func (p *SQSProducer) Publish(ctx context.Context, _ string, msg []byte) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &p.queueURL,
		MessageBody: aws.String(base64.StdEncoding.EncodeToString(msg)),
	})
	return err
}
