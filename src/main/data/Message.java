package main.data;

public class Message {
    private String topic;
    private String key;
    private String value;

    public Message(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }

    public String toString() {
        return "Topic: " + this.topic + ", Key: " + this.key + ", Value: " + this.value;
    }
}
