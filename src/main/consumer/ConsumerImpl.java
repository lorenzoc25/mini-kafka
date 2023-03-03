package main.consumer;

import main.broker.Broker;

import java.util.*;

public class ConsumerImpl implements Consumer {
    private Set<String> topics;
    private final String consumerId;
    private Broker broker;
    private Map<String, Integer> offset;

    public ConsumerImpl() {
        this.topics = new HashSet<>();
        this.consumerId = UUID.randomUUID().toString();
        this.offset = new HashMap<>();
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
        List<List<ConsumerRecord>> recordsByTopic = new ArrayList<>();
        List<ConsumerRecord> records = new ArrayList<>();

        for (String topic : this.topics) {
            recordsByTopic.add(this.broker.fetchTopicFor(topic, this.consumerId));
        }

        updateOffsetFromMessages(recordsByTopic);

        for (List<ConsumerRecord> currTopicRecords : recordsByTopic) {
            records.addAll(currTopicRecords);
        }

        return records;
    }

    private void updateOffsetFromMessages(List<List<ConsumerRecord>> recordsByTopic) {
        for (List<ConsumerRecord> currTopicRecords : recordsByTopic) {
            String currTopic = currTopicRecords.get(0).getMessage().getTopic();
            this.offset.put(currTopic, currTopicRecords.get(currTopicRecords.size() - 1).getOffset());
        }
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>(this.topics);
    }

    @Override
    public Integer getOffsetForTopic(String topic) {
        return this.offset.get(topic);
    }

    public Boolean commitOffsetForTopic(String topic, Integer offset) {
        if (offset < this.offset.get(topic)) {
            return false;
        } else if (!this.offset.containsKey(topic)
                || !this.topics.contains(topic)) {
            return false;
        }

        Boolean success = this.broker.commitOffset(topic, this.consumerId, offset);
        if (success) {
            this.offset.put(topic, 0);
        }
        return success;
    }
}
