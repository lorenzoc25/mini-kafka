package main.producer;

import main.data.Message;

public class ProducerRecord {
    private final Message message;
    private Integer partitionId;

    public ProducerRecord(Message message) {
        this.message = message;
        this.partitionId = null;
    }

    public ProducerRecord(
            Message message,
            Integer partitionId
    ) {
        this.message = message;
        this.partitionId = partitionId;
    }

    public Message toMessage() {
        return this.message;
    }

    public String topic() {
        return this.message.getTopic();
    }

    public String key() {
        return this.message.getKey();
    }

    public String value() {
        return this.message.getValue();
    }

    public Integer getPartitionId() {
        return this.partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

}