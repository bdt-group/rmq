rmq
=====

The library provides an interface for working with RabbitMQ.

Requirements
--------------------

The library is built under Ubuntu 18.04 and requires OTP 22, does not work on OTP 24 and above. For other versions of OTP, the work of the library has not been tested.

Building and testing
--------------------

Compile:
```shell
make compile 
```

Run tests with rabbitmq in local env (must be on localhost:5672 and have rmq:rmq user/password):
```shell
make test
```

Run tests with rabbitmq in docker-compose (requires OTP 22 and docker-compose):
```shell
make dc_test
```

Cleanup:
```shell
make clean
make distclean
make decompose #put down docker-compose stack used for tests
```

Usage
--------------------
This library explicitly specifies that the producer creates exchanges, the consumer creates queues. Therefore when interfacing via rabbitmq the necessary exchanges must be created by either external means(script or launch of other service which declares the necessary exchanges) or the service itself must create a producer before consumers.

Add library as dependency (rebar3 example):
```erlang
{deps, [
    {rmq, {git, "https://github.com/bdt-group/smpp", {branch, "master"}}},
]}.

```

An example of a typical connection to rabbitmq queues. 
Producer:
```erlang
URI = uri_string:parse("amqp://rmq:rmq@127.0.0.1:5672"),
ProducerConfig = 
    #{type => producer,
      exchange => <<"test">>,
      create => #{exchange_type => direct, %% type of exchange
                  exchange_durable => true, %% durability of exchange
                }},

%% add this childspec to supervisor of your choice
ProducerSpec = rmq:child_spec(URI, 
                              producer, %% erlang process name
                              ProducerConfig), 
```

Consumer:
```erlang
URI = uri_string:parse("amqp://rmq:rmq@127.0.0.1:5672"),
ConsumerConf = 
    #{queue => <<"test">>,
      handler => ?MODULE, %% module, implementing rmq_handler behaviour
      ack => true,
      type => consumer,
      create => #{exchange => <<"test">>,
                  queue_durable => true, %% durability of queue
                  queue_x_message_ttl => 60000, %% queue message TTL
                  queue_x_queue_type => quorum, %% quorum queue
                  queue_x_quorum_initial_group_size => 1 %% initial group size for quorum
                 }},

%% add this childspec to supervisor of your choice
ConsumerSpec = rmq:child_spec(URI, 
                              consumer, %% erlang process name
                              ConsumerConf) 
```
`rmq:child_spec/4` has an extra argument which receives arbitrary erlang term and passes it to 3d argument of handler callback.

Consumers' handling logic is implemented via behaviour `rmq_handler`, which defines callback `handle_msg/3`. `handle_msg` is called on each message fetched by consumer. Usage example:
```erlang
-module(example_handler).
-behaviour(rmq_handler).
-include_lib("kernel/include/logger.hrl").

-export([handle_msg/3]).

-spec handle_msg(rmq:routing_key(), binary(), _) -> ack.
handle_msg(RK,  %% message's routing key
           Payload, %% payload
           State %% term from 4th arg if declared by rmq:child_spec/4
          ) ->
    Event = binary_to_term(Payload),
    ?LOG_INFO("received event:~p, RK: ~p, state: ~p", [Event, RK, State]),
    ack.
```
