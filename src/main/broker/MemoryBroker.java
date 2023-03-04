package main.broker;

import main.consumer.ConsumerRecord;
import main.data.Message;
import main.data.Partition;
import main.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// TODO: INITIALIZE OFFSETS

public class MemoryBroker implements Broker {
    /**
     * Map topic -> List of partitions
     * <p>
     * Used to store all the messages. Each partition contains a list of
     * messages
     */
    private final Map<String, List<Partition>> records;
    /**
     * Map topic -> List of consumerIds
     * <p>
     * Used to keep track of the consumers that are subscribed to a topic
     */
    private final Map<String, List<String>> subscriptions;
    /**
     * Map topic -> PartitionId -> ConsumerId -> Offset
     * <p>
     * Used to keep track of the offset for each consumer
     */
    private final Map<String, Map<Integer, Map<String, Integer>>> offsets;

    public MemoryBroker() {
        this.records = new HashMap<>();
        this.subscriptions = new HashMap<>();
        this.offsets = new HashMap<>();
    }


    @Override
    public synchronized void store(ProducerRecord producerRecord) {
        String topic = producerRecord.getTopic();
        Integer partitionId = producerRecord.getPartitionId();
        if (this.records.get(topic) == null) {
            this.records.put(topic, new ArrayList<>());
            createPartition(topic);
        }
        addMessageToPartition(topic, partitionId, producerRecord.toMessage());
    }

    private Integer getPartitionsSize(String topic) {
        if (this.records.containsKey(topic)) {
            return this.records.get(topic).size();
        }
        return 0;
    }

    /**
     * Create a new partition for the given topic, returns the index
     * of the new partition(partitionID)
     *
     * @param topic - the topic to create a new partition for
     * @return partitionId
     */
    private Integer createPartition(String topic) {
        Integer numPartition = getPartitionsSize(topic);
        if (numPartition == 0) {
            this.records.put(topic, new ArrayList<>());
            this.offsets.put(topic, new HashMap<>());
        }
        this.records.get(topic).add(new Partition(numPartition));
        this.offsets.computeIfAbsent(topic, k -> new HashMap<>());
        this.offsets.get(topic).put(numPartition, new HashMap<>());
        return numPartition;
    }

    private void addMessageToPartition(String topic, Integer partitionId, Message message) {
        if (partitionId != null) {
            if (partitionId >= getPartitionsSize(topic)) {
                throw new RuntimeException("Partition does not exist");
            }
            this.records.get(topic).get(partitionId).addMessage(message);
        } else {
            Integer generatedPartitionId = generatePartitionIdFor(topic, message.getKey());
            this.records.get(topic).get(generatedPartitionId).addMessage(message);
        }
    }

    private Integer generatePartitionIdFor(String topic, String key) {
        // TODO: this should be in run in a background thread
        Integer numPartition = getPartitionsSize(topic);
        if (numPartition == 0) {
            return createPartition(topic);
        }
        Integer partitionId = hashKey(key, numPartition);
        return partitionId;
    }

    private Integer hashKey(String key, Integer range) {
        int hash = 7;
        for (char c : key.toCharArray()) {
            hash += hash * 31 + c;
        }
        return hash % range;
    }

    @Override
    public synchronized Message get(String topic, String key) {
        if (this.records.containsKey(topic)) {
            for (Partition partition : this.records.get(topic)) {
                for (Message message : partition.getMessages()) {
                    if (message.getKey().equals(key)) {
                        return message;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<ConsumerRecord> fetchTopicFor(String topic, String consumerId) {
        if (!this.records.containsKey(topic)) {
            return null;
        }
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        List<Partition> partitionsInTopic = this.records.get(topic);
        for (int partitionId = 0; partitionId < partitionsInTopic.size(); partitionId++) {
            Partition partition = partitionsInTopic.get(partitionId);
            List<Message> messagesInPartition = partition.getMessages();
            for (
                    int i = getOffsetFor(topic, consumerId, partitionId);
                    i < messagesInPartition.size();
                    i++
            ) {
                consumerRecords.add(
                        new ConsumerRecord(
                                messagesInPartition.get(i),
                                i,
                                partitionId
                        ));
            }
        }
        return consumerRecords;
    }

    private Integer getOffsetFor(
            String topic,
            String consumerId,
            Integer partitionId
    ) {
        if (this.offsets.containsKey(topic)) {
            return this.offsets
                    .get(topic)
                    .get(partitionId)
                    .getOrDefault(consumerId, 0);
        }
        return 0;
    }

    @Override
    public void addSubscription(String topic, String consumerId) {
        addConsumerToTopic(topic, consumerId);
        if (this.offsets.get(topic) == null) {
            createPartition(topic);
        }
        updateOffsetForConsumer(topic, consumerId, 0, getAssignedPartition(topic));
    }

    @Override
    public void addSubscription(String topic, String consumerId, Integer partitionId) {
        addConsumerToTopic(topic, consumerId);
        if (this.offsets.get(topic) == null) {
            createPartition(topic);
        }
        updateOffsetForConsumer(topic, consumerId, 0, partitionId);
    }

    private void addConsumerToTopic(String topic, String consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).add(consumerId);
        } else {
            this.subscriptions.put(topic, new ArrayList<>());
            this.subscriptions.get(topic).add(consumerId);
        }
    }

    /**
     * Assign a partition to a consumer. For now, it will choose the last
     * partition in the available partitions
     *
     * @param topic - the topic to assign a partition to
     * @return partitionId
     */
    private Integer getAssignedPartition(String topic) {
        if (this.records.containsKey(topic)) {
            return this.records.get(topic).size() - 1;
        }
        return 0;
    }

    private void updateOffsetForConsumer(
            String topic,
            String consumerId,
            int offset,
            Integer partitionId
    ) {
        if (
                !this.offsets.containsKey(topic) ||
                !this.offsets.get(topic).containsKey(partitionId)
        ) {
            throw new RuntimeException("Partition does not exist");
        }
        this.offsets
                .get(topic)
                .get(partitionId)
                .put(consumerId, offset);
    }

    @Override
    public void removeSubscription(String topic, String consumerId) {
        if (this.subscriptions.containsKey(topic)) {
            this.subscriptions.get(topic).remove(consumerId);
            for (Partition partition : this.records.get(topic)) {
                this.offsets
                        .get(topic)
                        .get(partition.getPartitionId())
                        .remove(consumerId);
            }
        }
    }

    @Override
    public synchronized Boolean commitOffset(
            String topic,
            String consumerId,
            int offset,
            Integer partitionId
    ) {
        updateOffsetForConsumer(topic, consumerId, offset, partitionId);
        return cleanOutdatedRecords(topic, partitionId);
    }

    private boolean cleanOutdatedRecords(String topic, Integer partitionId) {
        int minOffset = Integer.MAX_VALUE;
        for (String consumerId : this.subscriptions.get(topic)) {
            int offset = getOffsetFor(topic, consumerId, partitionId);
            minOffset = Math.min(minOffset, offset);
        }
        if (minOffset == Integer.MAX_VALUE) {
            return false;
        }

        // clean out all the offset that is less than minOffset of consumers
        this.records.get(topic).get(partitionId).updateMessageForOffset(minOffset + 1);
        // reset the offset to 0 for all consumer in this topic
        setTopicOffset(topic, 0, partitionId);
        return true;
    }

    private void setTopicOffset(String topic, int offset, Integer partitionId) {
        for (String consumerId : this.subscriptions.get(topic)) {
            updateOffsetForConsumer(topic, consumerId, offset, partitionId);
        }
    }

    public List<Message> getAllMessagesInTopic(String topic) {
        List<Message> messages = new ArrayList<>();
        for (Partition partition : this.records.get(topic)) {
            messages.addAll(partition.getMessages());
        }
        return messages;
    }

    public List<List<Message>> getAllMessageWithPartition(String topic) {
        List<List<Message>> messages = new ArrayList<>();
        for (Partition partition : this.records.get(topic)) {
            messages.add(partition.getMessages());
        }
        return messages;
    }

}
