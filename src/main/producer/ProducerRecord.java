package main.producer;

import main.data.Message;

public class ProducerRecord {
    private Message message;
    private Integer partitionId;
    /**
     * As for now, a producer record is just an encapsulation of a message, no other
     * information is added
     * @param topic
     * @param key
     * @param value
     */
    public ProducerRecord(String topic, String key, String value) {
        this.message = new Message(topic, key, value);
        this.partitionId = null;
    }

    public ProducerRecord(String topic, String key, String value, Integer partitionId) {
        this.message = new Message(topic, key, value);
        this.partitionId = partitionId;
    }

    public Message toMessage() {
        return this.message;
    }
    public String getTopic() {
        return this.message.getTopic();
    }
    public String getKey() {
        return this.message.getKey();
    }
    public String getValue() {
        return this.message.getValue();
    }

    public Integer getPartitionId() {
        return this.partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }
}