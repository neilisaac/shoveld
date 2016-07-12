# shoveld
Move RabbitMQ messages from point a to point b in style!


## build

Use Go 1.6+ or 1.5+ with GOVENDOREXPERIMENT=1

```
export GOPATH=`pwd`
go build shoveld
```


## configure

Annotated example:

```
name: fancy shovel
concurrency: 4
source:
  host: localhost
  vhost: fancy
  queue: upstream.out
  bindings:
    - exchange: upstream.in
      routingkey: fancy
sink:
  host: localhost
  vhost: spiffy
  exchange: spiffy.in
```

See `src/shoveld/config.go` for the complete set of options avialable.

run: `shoveld example.yaml`

Multiple file names may be provided to run multiple workers.
