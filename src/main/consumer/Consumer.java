package main.consumer;

import main.broker.Broker;

import java.util.List;

public interface Consumer {
    // can be either single consumer or a consumer group
    void connect(Broker broker);

    void subscribe(String topic);

    void subscribe(List<String> topics);

    void unsubscribe(String topic);

    List<ConsumerRecord> poll();

    List<String> getTopics();

    /**
     * Get the offset from the consumer - note this is different from the offset in
     * the broker since this would record the offset of the last message consumed
     * <p>
     * i.e. if there's 3 message in the broker, once the consumer polled all 3 of
     * them, the offset would be 2 (0-index) in this offset table.
     *
     * @param topic - the topic that the consumer wants to get the offset for
     * @param partitionId - the partition id that the consumer wants to get the offset for
     * @return int - the offset of the last message consumed
     */
    int getOffsetFor(String topic, Integer partitionId);

    Boolean commitOffsetFor(String topic, Integer partitionId, Integer offset);
}

