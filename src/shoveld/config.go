package main

import (
	"fmt"
    "io"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

var NumShovels = 0

// ShovelConfig represents the settings corresponding to a single shovel
type ShovelConfig struct {
	Name        string // friendly name for shovel
	Host        string
	Port        int
	VHost       string
	User        string
	Password    string
	Prefetch    int
	Concurrency int
	Source      ShovelSource
	Sink        ShovelSink
}

// ShovelSource represnets the source queue to read from.
// Exchange is optional and indicates an exchange to which the queue should be bound.
type ShovelSource struct {
	Queue     string
	Exchange  string
	Bindings  []ShovelSourceBinding
	Transient bool
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

func ParseShovel(reader io.Reader) ShovelConfig {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	shovel := ShovelConfig{}
	if err := yaml.Unmarshal(bytes, &shovel); err != nil {
		log.Fatal(err)
	}
	
	NumShovels++
	
	if shovel.Name == "" {
		shovel.Name = fmt.Sprintf("shovel%d", NumShovels)
	}
	if shovel.Host == "" {
		shovel.Host = "localhost"
	}
	if shovel.Port == 0 {
		shovel.Port = 5672
	}
	if shovel.VHost == "" {
		shovel.VHost = "/"
	}
	if shovel.User == "" {
		shovel.User = "guest"
	}
	if shovel.Password == "" {
		shovel.Password = "guest"
	}
	if shovel.Prefetch == 0 {
		shovel.Prefetch = 100
	}
	if shovel.Concurrency == 0 {
		shovel.Concurrency = 1
	}
	if shovel.Concurrency < 0 {
		log.Fatal("negative concurrency not allowed")
	}

	return shovel
}
