package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"

	"gopkg.in/yaml.v2"
)

var numShovels = 0

// ShovelConfig represents the settings corresponding to a single shovel
type ShovelConfig struct {
	Name        string // friendly name for shovel
	Concurrency int
	Source      ShovelSource
	Sink        ShovelSink
}

// SetDefaults assigns default values to blank fields
func (s *ShovelConfig) SetDefaults() {
	if s.Name == "" {
		s.Name = fmt.Sprintf("shovel%d", numShovels)
	}

	if s.Concurrency < 0 {
		log.Fatal("negative concurrency not allowed")
	}
	if s.Concurrency == 0 {
		s.Concurrency = 1
	}
}

// AMQPHost contains the host details required for an amqp connection
type AMQPHost struct {
	Host     string
	Port     int
	User     string
	Password string
	VHost    string
}

// URI returns an AMQP connection string.
func (h AMQPHost) URI() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", h.User, h.Password, h.Host, h.Port, url.QueryEscape(h.VHost))
}

// ShovelSource represnets the source queue to read from.
// Exchange is optional and indicates an exchange to which the queue should be bound.
type ShovelSource struct {
	AMQPHost `yaml:",inline"`
	Queue    string
	Bindings []ShovelSourceBinding
	Prefetch int
	// TODO: Transient bool
}

// ShovelSourceBinding represents a single binding to feed the input queue.
type ShovelSourceBinding struct {
	Exchange   string
	RoutingKey string
}

// ShovelSink represents the output of the shovel.
// RoutingKey is optional and overrides a message's routing key if specified.
type ShovelSink struct {
	AMQPHost     `yaml:",inline"`
	Exchange     string
	RoutingKey   string
	ExchangeType string
}

// ParseShovel parses a ShovelConfig from a given reader.
func ParseShovel(reader io.Reader) ShovelConfig {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	shovel := ShovelConfig{
		Name:        "", // required
		Concurrency: 1,
		Source: ShovelSource{
			AMQPHost: AMQPHost{
				Host:     "localhost",
				Port:     5672,
				VHost:    "/",
				User:     "guest",
				Password: "guest"},
			Queue:    "", // required
			Bindings: nil,
			Prefetch: 100},
		Sink: ShovelSink{
			AMQPHost: AMQPHost{
				Host:     "localhost",
				Port:     5672,
				VHost:    "/",
				User:     "guest",
				Password: "guest"},
			Exchange:     "", // required
			RoutingKey:   "",
			ExchangeType: "topic"}}

	if err := yaml.Unmarshal(bytes, &shovel); err != nil {
		log.Fatal(err)
	}

	numShovels++
	return shovel
}
