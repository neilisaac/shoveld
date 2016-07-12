package main

import (
	"log"

	"github.com/streadway/amqp"
)

type Worker struct {
	Config     ShovelConfig
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (w *Worker) Init() {
	connection, err := amqp.Dial(w.Config.Source.URI())
	if err != nil {
		log.Fatal(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}

	channel.Close()
	w.connection = connection
}

func (w *Worker) Work() {
	// see https://godoc.org/github.com/streadway/amqp#example-Channel-Confirm-Bridge

	//channel, err := w.connection.Channel()

}
