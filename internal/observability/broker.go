package observability

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker tracks an authenticated connection, reconnecting after broker loss.
// Future consumers must register on each newly established connection.
type Broker struct {
	URL                    string
	InitialDelay, MaxDelay time.Duration
	ready                  atomic.Bool
}

func (b *Broker) Ready() bool { return b.ready.Load() }

func (b *Broker) Run(ctx context.Context, firstReady chan<- struct{}) {
	initial, maximum := b.InitialDelay, b.MaxDelay
	if initial <= 0 {
		initial = time.Second
	}
	if maximum < initial {
		maximum = initial
	}
	backoff := initial
	first := true
	for ctx.Err() == nil {
		connection, err := amqp.DialConfig(b.URL, amqp.Config{Dial: func(network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}})
		if err == nil {
			b.ready.Store(true)
			backoff = initial
			if first {
				close(firstReady)
				first = false
			}
			closed := connection.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-ctx.Done():
			case <-closed:
			}
			b.ready.Store(false)
			_ = connection.Close()
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maximum {
			backoff = maximum
		}
	}
}

func (b *Broker) WaitReady(ctx context.Context, firstReady <-chan struct{}) error {
	select {
	case <-firstReady:
		return nil
	case <-ctx.Done():
		return errors.New("catalog-comparison RabbitMQ readiness timeout or shutdown")
	}
}
