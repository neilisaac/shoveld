package main

import (
    "io"

	"gopkg.in/yaml.v2"
)

// ShovelConfig represents the settings corresponding to a single shovel
type ShovelConfig struct {
	Host        string
	Port        int
	VHost       string
	Source      ShovelSource
	Sink        ShovelSink
	Concurrency int
}

// ShovelSource represnets the source queue to read from.
// Exchange is optional and indicates an exchange to which the queue should be bound.
type ShovelSource struct {
	Queue    string
	Exchange string
	Bindings []ShovelSourceBinding
}

// ShovelSourceBinding represents a single binding to feed the input queue.
type ShovelSourceBinding struct {
	Exchange   string
	RoutingKey string
}

// ShovelSink represents the output of the shovel.
// RoutingKey is optional and overrides a message's routing key if specified.
type ShovelSink struct {
	Exchange   string
	RoutingKey string
}

func ParseConfig(reader io.Reader) ShovelConfig {

}
