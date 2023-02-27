package test;
import main.broker.Broker;
import main.broker.MemoryBroker;
import main.consumer.Consumer;
import main.consumer.ConsumerImpl;
import main.data.Message;
import main.producer.Producer;
import main.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import main.*;

import java.util.ArrayList;
import java.util.List;


public class basicTest {
    @Test
    public void test() {
        Producer producer = new Producer("test");
        Broker broker = new MemoryBroker();
        producer.connectToBroker(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connectToBroker(broker);
        consumer.subscribe("test");
        producer.send(new ProducerRecord("test", "key", "value"));
        List<Message> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getKey(), "key");
        assertEquals(messages.get(0).getValue(), "value");

        messages.clear();
        consumer.unsubscribe("test");
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 0);

        System.out.printf("Basic test passed");
    }
}
