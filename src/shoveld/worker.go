package main

import (
	"fmt"
	"log"
	"net/url"
	
	"github.com/streadway/amqp"
)

type Worker struct {
	Config     ShovelConfig
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (w *Worker) Init() {
	uri := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
			w.Config.User, w.Config.Password,
			w.Config.Host, w.Config.Port,
			url.QueryEscape(w.Config.VHost))
			
	connection, err := amqp.Dial(uri)
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
