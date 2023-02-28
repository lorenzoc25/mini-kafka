package main.consumer;

import main.data.Message;

public class ConsumerRecord {
    private Integer offset;
    private Message message;

    public ConsumerRecord(Message message, Integer offset) {
        this.offset = offset;
        this.message = message;
    }

    public Integer getOffset() {
        return this.offset;
    }

    public Message getMessage() {
        return this.message;
    }
}
