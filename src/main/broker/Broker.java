package main.broker;

import main.consumer.ConsumerRecord;
import main.data.Message;

import java.util.List;


public interface Broker {
    /**
     * Store a message record in the broker
     * @param message - the message record to be stored
     */
    public void store(Message message);

    /**
     * Get a specific message record from a topic in the broker
     * @param topic - the topic that the message record is stored in
     * @param key - the key of the message record
     * @return Message - the message record
     */
    public Message get(String topic, String key);

    /**
     * Allow a consumer to subscribe to a topic from the broker
     * @param topic - the topic that the consumer wants to subscribe to
     * @param consumerId - the id of the consumer performing the subscription
     *
     */
    public void addSubscription(String topic, String consumerId);
    /**
     * Allow a consumer to unsubscribe to a topic from the broker
     * @param topic - the topic that the consumer wants to opt out of
     * @param consumerId - the id of the consumer performing the unsubscription
     *
     */
    public void removeSubscription(String topic, String consumerId);

    /**
     * Fetch all the message records for a consumer from a topic, offset
     * would be calculated by min(offset_record_in_broker, offset_record_in_consumer)
     * @param topic - the topic that the consumer wants to fetch from
     * @param consumerId - the id of the consumer performing the fetch
     * @return List<ConsumerRecord> - the list of message records for the consumer
     */
    public List<ConsumerRecord> getTopic(String topic, String consumerId);

    /**
     * Used for consumer to commit an offset, returns true if the offset is committed
     * and is therefore reset to 0
     * @param topic - the topic that the consumer wants to commit an offset for
     * @param consumerId - the id of the consumer performing the commit
     * @param offset - the offset that the consumer wants to commit
     * @return boolean - true if the offset is committed, false otherwise
     */
    public boolean commitOffset(String topic, String consumerId, int offset);
}
