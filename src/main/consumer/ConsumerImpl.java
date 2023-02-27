package main.consumer;

import main.broker.Broker;
import main.data.Message;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;

public class ConsumerImpl implements Consumer {
    private Set<String> topics;
    private final String consumerId;
    private Broker broker;

    public ConsumerImpl() {
        this.topics = new HashSet<>();
        this.consumerId = UUID.randomUUID().toString();
    }

    // like the main.producer, this is only a primitive implementation
    public void connectToBroker(Broker broker) {
        this.broker = broker;
    }
    @Override
    public void subscribe(String topic) {
        this.topics.add(topic);
        this.broker.addSubscription(topic, this.consumerId);
    }

    public void subscribe(List<String> topics) {
        for (String topic : topics) {
            this.subscribe(topic);
        }
    }
    public void unsubscribe(String topic) {
        this.topics.remove(topic);
        this.broker.removeSubscription(topic, this.consumerId);
    }

    public List<Message> poll() {
        List<Message> messages = new ArrayList<>();
        for (String topic : this.topics) {
            messages.addAll(this.broker.getTopic(topic));
        }
        return messages;
    }
}
