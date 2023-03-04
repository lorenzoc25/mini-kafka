# Mini Kafka

Mini Kafka is a simplified implementation of the Kafka message broker written in Java. It provides a basic
implementation of the Kafka producer and consumer APIs for sending and receiving messages.

This is only meant for educational and practice purposes and is not intended to be used in production.

## Todos
- [x] WIP: Add support for multiple partitions
  - A consumer can specify which partition to consume from
  - A producer can specify which partition to send to
  - Test to see if offsets are correctly managed by broker
  - Test to see if consumers can consume from multiple partitions
  
- [ ] Handle multithreaded producer/consumer requests
- [ ] Able to connect to real Kafka brokers
- [ ] Add support for consumer groups