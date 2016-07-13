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

	s.Source.SetDefaults()
	s.Sink.SetDefaults()
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

// SetDefaults assigns default values to blank fields
func (h *AMQPHost) SetDefaults() {
	if h.Host == "" {
		h.Host = "localhost"
	}
	if h.Port == 0 {
		h.Port = 5672
	}
	if h.VHost == "" {
		h.VHost = "/"
	}
	if h.User == "" {
		h.User = "guest"
	}
	if h.Password == "" {
		h.Password = "guest"
	}
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

// SetDefaults assigns default values to blank fields
func (s *ShovelSource) SetDefaults() {
	s.AMQPHost.SetDefaults()

	if s.Prefetch == 0 {
		s.Prefetch = 100
	}
}

// ShovelSourceBinding represents a single binding to feed the input queue.
type ShovelSourceBinding struct {
	Exchange   string
	RoutingKey string
}

// ShovelSink represents the output of the shovel.
// RoutingKey is optional and overrides a message's routing key if specified.
type ShovelSink struct {
	AMQPHost   `yaml:",inline"`
	Exchange   string
	RoutingKey string
}

// SetDefaults assigns default values to blank fields
func (s *ShovelSink) SetDefaults() {
	s.AMQPHost.SetDefaults()
}

// ParseShovel parses a ShovelConfig from a given reader.
func ParseShovel(reader io.Reader) ShovelConfig {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	shovel := ShovelConfig{}
	if err := yaml.Unmarshal(bytes, &shovel); err != nil {
		log.Fatal(err)
	}

	numShovels++

	shovel.SetDefaults()

	return shovel
}
