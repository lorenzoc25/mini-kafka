package broker;

import data.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryBroker implements Broker {
    private final Map<String, List<Message>> records;
    private final Map<String, List<String>> subscriptions;

    public MemoryBroker() {

        this.records = new HashMap<>();
        this.subscriptions = new HashMap<>();
    }

    public void store(Message message) {
        String topic = message.getTopic();
        if (this.records.containsKey(topic)) {
            this.records.get(topic).add(message);
        } else {
            this.records.put(topic, new ArrayList<Message>());
            this.records.get(topic).add(message);
        }
    }

    public Message get(String topic, String key) {
        if (this.records.containsKey(topic)) {
            for (Message message : this.records.get(topic)) {
                if (message.getKey().equals(key)) {
                    return message;
                }
            }
        }
        return null;
    }

    @Override
    public void distribute(String topic) {
        // find all the consumers that subscribed to the topic, and send the messages
    }

    @Override
    public void addSubscription(String topic, Integer consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).add(consumerId.toString());
        } else {
            this.subscriptions.put(topic, new ArrayList<String>());
            this.subscriptions.get(topic).add(consumerId.toString());
        }
    }

    @Override
    public void removeSubscription(String topic, Integer consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).remove(consumerId.toString());
        }
    }
}
