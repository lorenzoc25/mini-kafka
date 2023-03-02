package main.broker;

import main.consumer.ConsumerRecord;
import main.data.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class MemoryBroker implements Broker {
    private final Map<String, List<Message>> records;
    private final Map<String, List<String>> subscriptions;
    private final Map<String, Map<String, Integer>> offsets;

    public MemoryBroker() {
        this.records = new HashMap<>();
        this.subscriptions = new HashMap<>();
        this.offsets = new HashMap<>();
    }


    public synchronized void store(Message message) {
        String topic = message.getTopic();
        if (this.records.containsKey(topic)) {
            this.records.get(topic).add(message);
        } else {
            this.records.put(topic, new ArrayList<>());
            this.records.get(topic).add(message);
        }
    }

    public synchronized Message get(String topic, String key) {
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
    public List<ConsumerRecord> getTopic(String topic, String consumerId) {
        if (!this.records.containsKey(topic)) {
            return null;
        }
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        List<Message> messageInTopic = this.records.get(topic);
        for (int i = getTopicOffset(topic, consumerId); i < messageInTopic.size(); i++) {
            consumerRecords.add(
                    new ConsumerRecord(
                            messageInTopic.get(i),
                            i
                    ));
        }
        return consumerRecords;
    }

    private Integer getTopicOffset(String topic, String consumerId) {
        if (this.offsets.containsKey(topic)) {
            return this.offsets.get(topic).getOrDefault(consumerId, 0);
        }
        return 0;
    }

    @Override
    public void addSubscription(String topic, String consumerId) {
        addConsumerToTopic(topic, consumerId);
        updateOffsetForConsumer(topic, consumerId, 0);
    }

    private void addConsumerToTopic(String topic, String consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).add(consumerId);
        } else {
            this.subscriptions.put(topic, new ArrayList<>());
            this.subscriptions.get(topic).add(consumerId);
        }
    }

    private void updateOffsetForConsumer(
            String topic,
            String consumerId,
            int offset) {
        if (!this.offsets.containsKey(topic)) {
            this.offsets.put(topic, new HashMap<>());
        }
        this.offsets.get(topic).put(consumerId, offset);
    }

    @Override
    public void removeSubscription(String topic, String consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).remove(consumerId);
            this.offsets.get(topic).remove(consumerId);
        }
    }

    @Override
    public synchronized Boolean commitOffset(
            String topic,
            String consumerId,
            int offset) {
        updateOffsetForConsumer(topic, consumerId, offset);
        return cleanOutdatedRecords(topic);
    }
    private boolean cleanOutdatedRecords(String topic) {
        int minOffset = this.offsets.get(topic)
                .values()
                .stream()
                .mapToInt(Integer::intValue)
                .min()
                .orElse(Integer.MAX_VALUE);
        if (minOffset == Integer.MAX_VALUE) {
            return false;
        }
        // clean out all the offset that is less than minOffset of consumers
        List<Message> newRecords = this.records.get(topic).subList(
                minOffset+1,
                this.records.get(topic).size()
            );
        this.records.put(topic, newRecords);
        // reset the offset to 0 for all consumer in this topic
        setTopicOffset(topic, 0);
        return true;
    }

    private void setTopicOffset(String topic, int offset) {
        for (String consumerId : this.subscriptions.get(topic)) {
            updateOffsetForConsumer(topic, consumerId, offset);
        }
    }

    public List<Message> getAllMessagesInTopic(String topic){
        return this.records.get(topic);
    }

}
