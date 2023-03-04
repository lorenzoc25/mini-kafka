package main.consumer;

import main.broker.Broker;
import main.data.Message;

import java.util.*;

public class ConsumerImpl implements Consumer {
    private Set<String> topics;
    private final String consumerId;
    private Broker broker;
    /**
     * Map topic -> partitionId -> offset
     * <p>
     * The offset is the index of the next message to be consumed
     */
    private Map<String, Map<Integer, Integer>> offset;

    public ConsumerImpl() {
        this.topics = new HashSet<>();
        this.consumerId = UUID.randomUUID().toString();
        this.offset = new HashMap<>();
    }

    // like the producer, this is only a primitive implementation
    public void connect(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void subscribe(String topic) {
        this.topics.add(topic);
        this.broker.addSubscription(topic, this.consumerId);
        this.offset.put(topic, new HashMap<>());
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
        this.offset.remove(topic);
    }

    @Override
    public List<ConsumerRecord> poll() {
        List<List<ConsumerRecord>> recordsByTopic = new ArrayList<>();
        List<ConsumerRecord> records = new ArrayList<>();

        for (String topic : this.topics) {
            recordsByTopic.add(this.broker.fetchTopicFor(topic, this.consumerId));
        }

        updateOffsetFromMessages(
                transformToTopicPartitionMap(recordsByTopic)
        );

        for (List<ConsumerRecord> currTopicRecords : recordsByTopic) {
            records.addAll(currTopicRecords);
        }

        return records;
    }

    private void updateOffsetFromMessages(Map<String, Map<Integer, List<ConsumerRecord>>> topicPartitionMap) {
        for (String currTopic : topicPartitionMap.keySet()) {
            Map<Integer, List<ConsumerRecord>> currTopicPartitionMap = topicPartitionMap.get(currTopic);
            for (Integer partitionId : currTopicPartitionMap.keySet()) {
                List<ConsumerRecord> currPartition = currTopicPartitionMap.get(partitionId);
                this.offset
                        .get(currTopic)
                        .put(
                                partitionId,
                                currPartition.get(currPartition.size() - 1).getOffset()
                        );
            }
        }

    }

    /**
     * This method is used to transform the list of records by topic into a map of topic -> partitionId -> List of messages
     * @param recordsByTopic List of list<record>, separated by topic
     * @return Map of topic -> partitionId -> List of messages
     */
    private Map<String, Map<Integer, List<ConsumerRecord>>> transformToTopicPartitionMap(
            List<List<ConsumerRecord>> recordsByTopic
    ) {
        Map<String, Map<Integer, List<ConsumerRecord>>> topicPartitionMap = new HashMap<>();

        for (List<ConsumerRecord> currTopicRecords : recordsByTopic) {
            String currTopic = currTopicRecords.get(0).getMessage().getTopic();
            topicPartitionMap.put(currTopic, new HashMap<>());

            for (ConsumerRecord currRecord : currTopicRecords) {
                Integer currPartitionId = currRecord.getPartitionId();

                if (!topicPartitionMap.get(currTopic).containsKey(currPartitionId)) {
                    topicPartitionMap.get(currTopic).put(currPartitionId, new ArrayList<>());
                }

                topicPartitionMap.get(currTopic).get(currPartitionId).add(currRecord);
            }
        }

        return topicPartitionMap;
    }



    @Override
    public List<String> getTopics() {
        return new ArrayList<>(this.topics);
    }

    @Override
    public int getOffsetFor(String topic, Integer partitionId) {
        return this.offset.get(topic).get(partitionId);
    }

    @Override
    public Boolean commitOffsetFor(String topic, Integer partitionId, Integer offset) {
        if (offset < getOffsetFor(topic, partitionId)) {
            return false;
        } else if (!this.offset.containsKey(topic)
                || !this.topics.contains(topic)) {
            return false;
        }

        Boolean success = this.broker.commitOffset(topic, this.consumerId, offset, partitionId);
        if (success) {
            this.offset.get(topic).put(partitionId, 0);
        }
        return success;
    }
}
