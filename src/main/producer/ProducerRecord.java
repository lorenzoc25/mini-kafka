package main.producer;

import main.data.Message;

public class ProducerRecord {
    private main.data.Message message;
    /**
     * As for now, a producer record is just an encapsulation of a message, no other
     * information is added
     * @param topic
     * @param key
     * @param value
     */
    public ProducerRecord(String topic, String key, String value) {
        this.message = new Message(topic, key, value);
    }

    public Message toMessage() {
        return this.message;
    }
}