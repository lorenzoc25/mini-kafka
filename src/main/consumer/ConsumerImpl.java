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

    private Integer offset;

    public ConsumerImpl() {
        this.topics = new HashSet<>();
        this.consumerId = UUID.randomUUID().toString();
    }

    // like the main.producer, this is only a primitive implementation
    public void connect(Broker broker) {
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
    @Override
    public void unsubscribe(String topic) {
        this.topics.remove(topic);
        this.broker.removeSubscription(topic, this.consumerId);

    }

    @Override
    public List<ConsumerRecord> poll() {
        List<ConsumerRecord> records = new ArrayList<>();
        for (String topic : this.topics) {
            records.addAll(this.broker.getTopic(topic, this.consumerId));
        }
        return records;
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>(this.topics);
    }

    @Override
    public Integer getOffset(String topic) {
        return this.offset;
    }
}
