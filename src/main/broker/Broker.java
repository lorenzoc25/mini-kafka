package main.broker;

import main.data.Message;

import java.util.List;


public interface Broker {
    public void store(Message message);

    public Message get(String topic, String key);

    /**
     * @param topic the topic to distribute
     *              <p>
     *              This method is used to distribute the messages in the topic to the consumers that subscribed to the topic.
     */
    public void distribute(String topic);

    public void addSubscription(String topic, String consumerId);

    public void removeSubscription(String topic, String consumerId);

    public List<Message> getTopic(String topic);
}
