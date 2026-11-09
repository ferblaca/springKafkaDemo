# Spring Kafka `InvalidPidMappingException` demo

This project contains a minimal reproduction of the producer lifecycle behavior
when an idempotent, non-transactional producer receives an
`InvalidPidMappingException`.

## Versions

The demo uses the versions managed by Spring Boot 4.1.2:

- Spring Kafka 4.1.2
- Apache Kafka Clients 4.2.2

## Reproduction

Run:

```bash
./mvnw test -Dtest=InvalidPidMappingProducerFactoryTest
```

The first test simulates the asynchronous Kafka producer callback returning an
`InvalidPidMappingException`. It verifies that `DefaultKafkaProducerFactory`
continues returning the same cached producer and does not close it automatically.

The second test calls the public `DefaultKafkaProducerFactory.reset()` API and
verifies that the failed producer is closed and the next producer is a new
instance.

The test deliberately uses a controlled producer double so that it isolates
Spring Kafka's `DefaultKafkaProducerFactory` behavior. It does not depend on an
application-specific framework, broker, binder or platform.
