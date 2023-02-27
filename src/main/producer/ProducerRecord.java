package main.producer;

import main.data.Message;

public class ProducerRecord {
    private main.data.Message message;
    public ProducerRecord(String topic, String key, String value) {
        this.message = new Message(topic, key, value);
    }

    public Message toMessage() {
        return this.message;
    }
}