package main.broker;

import main.consumer.ConsumerRecord;
import main.data.Message;
import main.producer.ProducerRecord;

import java.util.List;


public interface Broker {
    /**
     * Store a producer record in the broker
     */
    void store(ProducerRecord producerRecord);

    /**
     * Get a specific message record from a topic in the broker
     *
     * @param topic - the topic that the message record is stored in
     * @param key   - the key of the message record
     * @return Message - the message record
     */
    Message get(String topic, String key);

    /**
     * Allow a consumer to subscribe to a topic from the broker. If partition
     * id is not specified, the broker will assign a partition to the consumer
     *
     * @param topic      - the topic that the consumer wants to subscribe to
     * @param consumerId - the id of the consumer performing the subscription
     */
    void addSubscription(String topic, String consumerId);

    void addSubscription(String topic, String consumerId, Integer partitionId);

    /**
     * Allow a consumer to unsubscribe to a topic from the broker
     *
     * @param topic      - the topic that the consumer wants to opt out of
     * @param consumerId - the id of the consumer performing the unsubscription
     */
    void removeSubscription(String topic, String consumerId);

    /**
     * Fetch all the message records for a consumer from a topic.
     * Offset will be calculated using the broker's offset map
     *
     * @param topic      - the topic that the consumer wants to fetch from
     * @param consumerId - the id of the consumer performing the fetch
     * @return List<ConsumerRecord> - the list of message records for the consumer
     */
    List<ConsumerRecord> fetchTopicFor(String topic, String consumerId);

    /**
     * Used for consumer to commit an offset, returns true if the offset is committed
     * and is therefore reset to 0
     *
     * @param topic      - the topic that the consumer wants to commit an offset for
     * @param consumerId - the id of the consumer performing the commit
     * @param offset     - the offset that the consumer wants to commit
     * @return boolean - true if the offset is committed, false otherwise
     */
    Boolean commitOffset(String topic, String consumerId, int offset, Integer partitionId);

    /**
     * Get all the messages in a topic without caring about offset
     *
     * @param topic - the desired topic
     * @return List<Message> - the list of message records
     */
    List<Message> getAllMessagesInTopic(String topic);

    /**
     * Get all the messages in a topic with partition
     * size of the list is the number of partitions, and each element of the
     * list is a list of messages in that partition
     *
     * @param topic
     * @return
     */
    List<List<Message>> getAllMessageWithPartition(String topic);
}
