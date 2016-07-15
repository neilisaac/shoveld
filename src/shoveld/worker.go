package main

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

// Worker does shoveling.
type Worker struct {
	ShovelConfig
	sourceConnection *amqp.Connection
	sourceChannel    *amqp.Channel
	sinkConnection   *amqp.Connection
	sinkChannel      *amqp.Channel
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

	for _, binding := range w.Source.Bindings {
		if binding.Exchange == "" {
			log.Fatal("exchange missing from source binding for: ", w.Name)
		}
		if err := channel.QueueBind(w.Source.Queue, binding.RoutingKey, binding.Exchange, false, nil); err != nil {
			log.Fatal(err)
		}
	}

	if err := channel.Qos(w.Source.Prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	w.sourceConnection = connection
	w.sourceChannel = channel
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

	if err := channel.ExchangeDeclare(w.Sink.Exchange, w.Sink.ExchangeType, true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}

	w.sinkConnection = connection
	w.sinkChannel = channel
}

// Init initializes the worker's source and connections, and establishes bindings.
func (w *Worker) Init() {
	w.initSource()
	w.initSink()
}

// Work does the shoveling and handles reconnecting as needed.
func (w *Worker) Work() {
	for {
		if w.sourceConnection == nil || w.sinkConnection == nil {
			w.Init()
		}

		err := w.doShoveling()
		if err != nil {
			log.Fatal(err)
		}

		w.sourceConnection.Close()
		w.sourceConnection = nil
		w.sinkConnection.Close()
		w.sinkConnection = nil
	}
}

func (w *Worker) doShoveling() error {
	// see https://godoc.org/github.com/streadway/amqp#example-Channel-Confirm-Bridge

	source := w.sourceChannel
	sink := w.sinkChannel

	shovel, err := source.Consume(w.Source.Queue, w.Name, false, false, false, false, nil)
	if err != nil {
		log.Panic(err)
		return err
	}

	confirms := sink.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := sink.Confirm(false); err != nil {
		log.Fatal(err)
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
			msg.Nack(false, true)
			log.Panic(err)
		}

		if confirmed := <-confirms; confirmed.Ack {
			msg.Ack(false)
		} else {
			msg.Nack(false, true)
		}
	}
}
