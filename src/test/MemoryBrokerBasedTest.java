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
        final String source = "basicRoutineTest";
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        Message msg1 = new Message(source, "test", "key", "value");
        producer.send(new ProducerRecord(msg1));
        List<ConsumerRecord> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).message().getKey(), "key");
        assertEquals(messages.get(0).message().getValue(), "value");

        Message msg2 = new Message(source, "test2", "key", "value");
        producer.send(new ProducerRecord(msg2));
        Message msg3 = new Message(source, "test3", "key", "value");
        producer.send(new ProducerRecord(msg3));
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
        final String source = "singlePartitionOffsetTest";
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        Message msg1 = new Message(source, "test", "key1", "value1");
        producer.send(new ProducerRecord(msg1));
        List<ConsumerRecord> messages = new ArrayList<>();
        messages.addAll(consumer.poll());
        assertEquals(messages.size(), 1);
        Integer currentOffset = consumer.getOffsetFor("test", partitionId);
        assertEquals(currentOffset, (Object) 0);
        Message msg2 = new Message(source, "test", "key2", "value2");
        producer.send(new ProducerRecord(msg2));
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
        final String source = "multiplePartitionTest";
        // each topic by default has 1 partition
        assertEquals(broker.getNumPartitions("test"), 1);
        // try to add more partitions
        broker.addPartition("test");
        assertEquals(broker.getNumPartitions("test"), 2);
        broker.addPartition("test");
        assertEquals(broker.getNumPartitions("test"), 3);
        // now send messages to the topic
        Message msg1 = new Message(source, "test", "key1", "value1");
        producer.send(new ProducerRecord(msg1));
        List<Message> messages = new ArrayList<>();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getKey(), "key1");
        Message msg2 = new Message(source, "test", "key2", "value2");
        Message msg3 = new Message(source, "test", "key3", "value3");
        Message msg4 = new Message(source, "test", "key4", "value4");
        producer.send(new ProducerRecord(msg2));
        producer.send(new ProducerRecord(msg3));
        producer.send(new ProducerRecord(msg4));
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 4);
        List<List<Message>> partitions = broker.getAllMessageWithPartition("test");
        assertEquals(partitions.size(), 3);

    }

    @Test
    public void multiplePartitionOffsetTest() {
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        consumer.subscribe("test");
        final String source = "multiplePartitionOffsetTest";
        Message msg1 = new Message(source, "test", "key0", "value0");
        Message msg2 = new Message(source, "test", "key1", "value1");
        Message msg3 = new Message(source, "test", "key2", "value2");
        producer.send(new ProducerRecord(msg1, 0));
        producer.send(new ProducerRecord(msg2,1));
        producer.send(new ProducerRecord(msg3, 2));
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
    public void singlePartitionTest() {
        Producer producer = new Producer();
        Broker broker = new MemoryBroker();
        producer.connect(broker);
        Consumer consumer = new ConsumerImpl();
        consumer.connect(broker);
        final String source = "singlePartitionTest";
        consumer.subscribe("test");

        // test out more than one message in a partition
        Message msg1 = new Message(source, "test", "key0", "value0");
        Message msg2 = new Message(source, "test", "key1", "value1");
        Message msg3 = new Message(source, "test", "key2", "value2");

        producer.send(new ProducerRecord(msg1, 0));
        producer.send(new ProducerRecord(msg2, 0));
        producer.send(new ProducerRecord(msg3, 0));

        List<Message> messages = new ArrayList<>(broker.getAllMessagesInTopic("test"));
        assertEquals(messages.size(), 3);

        assertEquals(0, consumer.getOffsetFor("test", 0));
        List<ConsumerRecord> consumerRecords = new ArrayList<>(consumer.poll());
        assertEquals(consumerRecords.size(), 3);

        consumer.commitOffsetFor("test", 0, 1);
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));

        assertEquals(consumer.getOffsetFor("test", 0), 0);
        messages.clear();
        messages.addAll(broker.getAllMessagesInTopic("test"));
        assertEquals(1, messages.size());
        assertEquals("key2", messages.get(0).getKey());
    }
}

