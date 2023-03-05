package test;

import main.broker.Broker;
import main.broker.MemoryBroker;
import main.consumer.Consumer;
import main.consumer.ConsumerImpl;
import main.consumer.ConsumerRecord;
import main.data.Message;
import main.producer.Producer;
import main.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class MemoryBrokerBasedTest {
    @Test
    public void basicRoutineTest() {
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
        assertEquals(messages.get(0).message().getKey(), "key");
        assertEquals(messages.get(0).message().getValue(), "value");

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
    }

    @Test
    public void singlePartitionOffsetTest() {
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        final int partitionId = 0;
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        producer.send(new ProducerRecord("test", "key1", "value1"));
        List<ConsumerRecord> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        Integer currentOffset = consumer.getOffsetFor("test", partitionId);
        assertEquals(currentOffset, (Object) 0);
        producer.send(new ProducerRecord("test", "key2", "value2"));
        assertEquals(broker.getAllMessagesInTopic("test").size(), 2);
        Boolean commitSuccess = consumer.commitOffsetFor("test", 0, 0);
        assertEquals(commitSuccess, true);
        // now the topic "test" should only have one message, since the first one
        // is cleaned up by the commit action
        assertEquals(broker.getAllMessagesInTopic("test").size(), 1);
        assertEquals(broker.getAllMessagesInTopic("test").get(0).getKey(), "key2");
        // now the offset should still reset to 0
        assertEquals(consumer.getOffsetFor("test", partitionId), 0);
    }

    @Test
    public void multiplePartitionTest() {
        // in Kafka, the partition is managed by the administrator, so for now, we will
        // just operate on the broker level
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        // each topic by default has 1 partition
        assertEquals(broker.getNumPartitions("test"), 1);
        // try to add more partitions
        broker.addPartition("test");
        assertEquals(broker.getNumPartitions("test"), 2);
        broker.addPartition("test");
        assertEquals(broker.getNumPartitions("test"), 3);
        // now send messages to the topic
        producer.send(new ProducerRecord("test", "key1", "value1"));
        List<Message> messages = new ArrayList<>();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getKey(), "key1");
        producer.send(new ProducerRecord("test", "key2", "value2"));
        producer.send(new ProducerRecord("test", "key3", "value2"));
        producer.send(new ProducerRecord("test", "key4", "value5"));
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 4);
        List<List<Message>> partitions = broker.getAllMessageWithPartition("test");
        assertEquals(partitions.size(), 3);
//        for (int i = 0; i < partitions.size(); i++) {
//            System.out.printf("Partition %d has %d messages%n", i, partitions.get(i).size());
//            for (Message message : partitions.get(i)) {
//                System.out.printf("Message: %s%n", message);
//            }
//        }
    }

    @Test
    public void multiplePartitionOffsetTest() {
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        producer.send(new ProducerRecord("test", "key0", "value0", 0));
        producer.send(new ProducerRecord("test", "key1", "value1", 1));
        producer.send(new ProducerRecord("test", "key2", "value2", 2));
        assertEquals(broker.getNumPartitions("test"), 3);
        List<Message> messages = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 3);

        List<ConsumerRecord> consumerRecords = new ArrayList<>(consumer.poll());
        assertEquals(consumerRecords.size(), 3);
        int offset0 = consumer.getOffsetFor("test", 0);
        assertEquals(offset0, 0);
        int offset1 = consumer.getOffsetFor("test", 1);
        assertEquals(offset1, 0);
        int offset2 = consumer.getOffsetFor("test", 2);
        assertEquals(offset2, 0);

        // now commit the offset
        Boolean commit0Success = consumer.commitOffsetFor("test", 0, 0);
        assertEquals(commit0Success, true);
        // now the offset should still reset to 0
        assertEquals(consumer.getOffsetFor("test", 0), 0);
        List<Message> messagesAfterCommit0 = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messagesAfterCommit0.size(), 2);

        Boolean commit1Success = consumer.commitOffsetFor("test", 1, 0);
        assertEquals(commit1Success, true);
        assertEquals(consumer.getOffsetFor("test", 1), 0);
        List<Message> messagesAfterCommit1 = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messagesAfterCommit1.size(), 1);

        Boolean commit2Success = consumer.commitOffsetFor("test", 2, 0);
        assertEquals(commit2Success, true);
        assertEquals(consumer.getOffsetFor("test", 2), 0);
        List<Message> messagesAfterCommit2 = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messagesAfterCommit2.size(), 0);

    }

    @Test
    public void test(){
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");

        // test out more than one message in a partition
        producer.send(new ProducerRecord("test", "key0", "value0", 0));
        producer.send(new ProducerRecord("test", "key1", "value1", 0));
        producer.send(new ProducerRecord("test", "key2", "value2", 0));

        List<Message> messages = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 3);

        assertEquals(0, consumer.getOffsetFor("test", 0));
        List<ConsumerRecord> consumerRecords = new ArrayList<>(consumer.poll());
        assertEquals(consumerRecords.size(), 3);

        consumer.commitOffsetFor( "test", 0, 1);
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));

        assertEquals(consumer.getOffsetFor("test", 0), 0);
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(1, messages.size());
        assertEquals("key2", messages.get(0).getKey());
    }
}

