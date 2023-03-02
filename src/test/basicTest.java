package test;
import main.broker.Broker;
import main.broker.MemoryBroker;
import main.consumer.Consumer;
import main.consumer.ConsumerImpl;
import main.consumer.ConsumerRecord;
import main.producer.Producer;
import main.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class basicTest {
    @Test
    public void test() {
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        producer.send(new ProducerRecord("test", "key", "value"));
        List<ConsumerRecord> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getMessage().getKey(), "key");
        assertEquals(messages.get(0).getMessage().getValue(), "value");

        producer.send(new ProducerRecord("test2", "key", "value"));
        producer.send(new ProducerRecord("test3", "key", "value"));
        consumer.subscribe(List.of("test2", "test3"));
        assertEquals(consumer.getTopics().size(), 3);
        List<ConsumerRecord> pollResult = consumer.poll();
        assertEquals(pollResult.size(), 3);

        messages.clear();
        consumer.unsubscribe("test");
        consumer.unsubscribe("test2");
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);

        System.out.printf("Basic test passed");
    }

    @Test
    public void offsetTest(){
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        producer.send(new ProducerRecord("test", "key1", "value1"));
        List<ConsumerRecord> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        Integer currentOffset = consumer.getOffsetForTopic("test");
        assertEquals(currentOffset, Optional.of(0).get());
        producer.send(new ProducerRecord("test", "key2", "value2"));
        assertEquals(broker.getAllMessagesInTopic("test").size(), 2);
        Boolean commitSuccess = consumer.commitOffsetForTopic("test", 0);
        assertEquals(commitSuccess, true);
        // now the topic "test" should only have one message, since the first one
        // is cleaned up by the commit action
        assertEquals(broker.getAllMessagesInTopic("test").size(), 1);
        assertEquals(broker.getAllMessagesInTopic("test").get(0).getKey(), "key2");
        // now the offset should still reset to 0
        assertEquals(consumer.getOffsetForTopic("test"), Optional.of(0).get());
    }
}
