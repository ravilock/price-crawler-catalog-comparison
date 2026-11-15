package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RequestReply publishes one durable request and waits for a correlated reply on
// an exclusive queue. The deadline bounds broker interaction.
func RequestReply(parent context.Context, url, queue string, request Envelope, timeout time.Duration) (Envelope, error) {
	if queue == "" || timeout <= 0 {
		return Envelope{}, errors.New("queue and positive timeout are required")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	conn, err := amqp.Dial(url)
	if err != nil {
		return Envelope{}, err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return Envelope{}, err
	}
	defer func() { _ = ch.Close() }()
	replyQueue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return Envelope{}, err
	}
	replies, err := ch.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		return Envelope{}, err
	}
	body, err := request.Encode()
	if err != nil {
		return Envelope{}, err
	}
	if err = ch.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     request.MessageID,
		CorrelationId: request.CorrelationID,
		ReplyTo:       replyQueue.Name,
		Body:          body,
	}); err != nil {
		return Envelope{}, fmt.Errorf("publish request: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case delivery, ok := <-replies:
			if !ok {
				return Envelope{}, errors.New("reply queue closed")
			}
			if delivery.CorrelationId != request.CorrelationID {
				continue
			}
			return Decode(delivery.Body)
		}
	}
}
