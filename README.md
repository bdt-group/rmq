rmq
=====

The library provides an interface for working with RabbitMQ.

Requirements
--------------------

The library is built under Ubuntu 18.04 and requires OTP 22, does not work on OTP 24 and above. For other versions of OTP, the work of the library has not been tested.

To run library tests, you need to raise the rabbitmq image via docker-compose.yml

Usage in development
--------------------

An example of a typical connection to rabbitmq queues. 
```
ProducerSpec = rmq:child_spec(URI, producer, config(producer))
ConsumerSpec = rmq:child_spec(URI, consumer, config(consumer))
```

This library automatically determines that the producer creates exchanges, the consumer creates queues.
