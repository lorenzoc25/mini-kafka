package main.consumer;

import main.data.Message;

public class ConsumerRecord {
    private Message message;
    private Integer offset;
    private Integer partitionId;

    public ConsumerRecord(Message message, Integer offset, Integer partitionId) {
        this.offset = offset;
        this.message = message;
        this.partitionId = partitionId;
    }

    public Integer offset() {
        return this.offset;
    }

    public Message message() {
        return this.message;
    }


    public Integer partition() {
        return this.partitionId;
    }
}
