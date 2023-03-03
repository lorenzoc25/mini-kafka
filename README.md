# Mini Kafka

Mini Kafka is a simplified implementation of the Kafka message broker written in Java. It provides a basic
implementation of the Kafka producer and consumer APIs for sending and receiving messages.

This is only meant for educational and practice purposes and is not intended to be used in production.

## Todos
- [ ] Add support for multiple partitions
  - Needs to add partition in the consumer record
  - Test if the original testcase still works
  - A consumer can specify which partition to consume from
  
- [ ] Handle multithreaded producer/consumer requests
- [ ] Able to connect to real Kafka brokers
- [ ] Add support for consumer groups