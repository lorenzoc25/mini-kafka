package broker;

import data.Message;


public interface Broker {
    public void store(Message message);
    public Message get(String topic, String key);

    /**
     * @param topic the topic to distribute
     *
     *  This method is used to distribute the messages in the topic to the consumers that subscribed to the topic.
     */
    public void distribute(String topic);
    public void addSubscription(String topic, Integer consumerId);
    public void removeSubscription(String topic, Integer consumerId);

}
