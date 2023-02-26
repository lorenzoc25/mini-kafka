package producer;

import data.Message;

public class ProducerRecord {
    private data.Message message;
    public ProducerRecord(String topic, String key, String value) {
        this.message = new Message(topic, key, value);
    }

    public Message toMessage() {
        return this.message;
    }
}