# Mini Kafka

Mini Kafka is a simplified implementation of the Kafka message broker written in Java. It provides a basic
implementation of the Kafka producer and consumer APIs for sending and receiving messages.

This is only meant for educational and practice purposes and is not intended to be used in production.

## Todos

- [x] Add support for multiple partitions
    - ~~A consumer can specify which partition to consume from~~ Client using the consumer should be able to handle
      this, since partition ID will be available when polling
    - [x] A producer can specify which partition to send to
    - [x] Test to see if offsets are correctly managed by broker

- [ ] Handle multithreaded producer/consumer requests
- [ ] Add support for consumer groups
- [ ] Implement log-based storage broker for real usages
- [ ] Able to connect to real Kafka brokers

## Usage

```java
import main.data.Message;
import main.producer.Producer;

class Example {
    
    // right now, only broker implemented is the in-memory broker
    private Broker broker = new MemoryBroker();
    
    public static void producerExample() {
        Producer producer = new Producer();
        producer.connect(this.broker);

        /**
         * We can create a message with topic and KV pair, which
         * is built on top of CloudEvent, so we can also construct
         * a Message by passing in (topic, CloudEvent) as well.
         */
        Message msg = new Message("test", "key1", "value1");
        
        // send a test message to the topic "test"
        producer.send(new ProducerRecord(msg));
        // send a test message with a specified partition
        producer.send(new ProducerRecord(msg, 0));
    }

    public static void consumerExample() {
        Consumer consumer = new Consumer();
        consumer.connect(this.broker);
        // subscribe to the topic "test"
        consumer.subscribe("test");
        // poll for messages
        ConsumerRecords records = consumer.poll();
        // print out the messages
        for (ConsumerRecord record : records) {
            System.out.println(record);
        }
        // already consumed the first message, so commit the offset
        consumer.commitOffsetFor("test", 0);
    }
}
```