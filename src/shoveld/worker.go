package main

import (
	"errors"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

// Worker does shoveling.
type Worker struct {
	ShovelConfig
	sourceConnection *amqp.Connection
	sinkConnection   *amqp.Connection
}

func (w *Worker) initSource() {
	connection, err := amqp.Dial(w.Source.URI())
	if err != nil {
		log.Fatal(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := channel.QueueDeclare(w.Source.Queue, true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	defer channel.Close()

	for _, binding := range w.Source.Bindings {
		if binding.Exchange == "" {
			log.Fatal("exchange missing from source binding for", w.Name)
		}
		if binding.RoutingKey == "" {
			log.Fatal("routing key missing from source binding for", w.Name)
		}
		if err := channel.QueueBind(w.Source.Queue, binding.RoutingKey, binding.Exchange, false, nil); err != nil {
			log.Fatal(err)
		}
	}

	channel.Qos(w.Source.Prefetch, w.Source.Prefetch, true)

	w.sourceConnection = connection
}

func (w *Worker) initSink() {
	connection, err := amqp.Dial(w.Sink.URI())
	if err != nil {
		log.Fatal(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(w.Sink.Exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}

	w.sinkConnection = connection
}

// Init initializes the worker's source and connections, and establishes bindings.
func (w *Worker) Init() {
	w.initSource()
	w.initSink()
}

// Work does the shoveling.
func (w *Worker) Work() {
	var wg sync.WaitGroup
	wg.Add(w.Concurrency)

	for i := 0; i < w.Concurrency; i++ {
		go func() {
			defer wg.Done()
			err := w.doShoveling()
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	wg.Wait()
}

func (w *Worker) doShoveling() error {
	// see https://godoc.org/github.com/streadway/amqp#example-Channel-Confirm-Bridge

	source, err := w.sourceConnection.Channel()
	if err != nil {
		return err
	}
	defer source.Close()

	sink, err := w.sinkConnection.Channel()
	if err != nil {
		return err
	}
	defer sink.Close()

	shovel, err := source.Consume(w.Source.Queue, w.Name, false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		msg, ok := <-shovel
		if !ok {
			return errors.New("source channel closed")
		}

		routingKey := msg.RoutingKey
		if w.Sink.RoutingKey != "" {
			routingKey = w.Sink.RoutingKey
		}

		err := sink.Publish(w.Sink.Exchange, routingKey, false, false, amqp.Publishing{
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			DeliveryMode:    msg.DeliveryMode,
			Priority:        msg.Priority,
			CorrelationId:   msg.CorrelationId,
			ReplyTo:         msg.ReplyTo,
			Expiration:      msg.Expiration,
			MessageId:       msg.MessageId,
			Timestamp:       msg.Timestamp,
			Type:            msg.Type,
			UserId:          msg.UserId,
			AppId:           msg.AppId,
			Headers:         msg.Headers,
			Body:            msg.Body})

		if err != nil {
			msg.Nack(false, false)
			return err
		}

		msg.Ack(false)
	}
}
