package main.data;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class Message {
    private final CloudEvent cloudEvent;
    private final String topic;

    /**
     * Creates a message that contains the cloud event
     *
     * @param topic      topic name
     * @param cloudEvent cloud event
     */
    public Message(String topic, CloudEvent cloudEvent) {
        this.topic = topic;
        this.cloudEvent = cloudEvent;
    }

    /**
     * Creates a key-value typed message
     *
     * @param topic topic name
     * @param key   message key
     * @param value message value
     */
    public Message(String sourceId, String topic, String key, String value) {
        this.topic = topic;
        this.cloudEvent = CloudEventBuilder.v1()
                .withId(key)
                .withType("kv-message")
                .withSource(URI.create(sourceId))
                .withData("text/plain",value.getBytes())
                .build();
    }

    public String getTopic() {
        return this.topic;
    }

    public String getKey() {
        return this.cloudEvent.getId();
    }

    public String getValue() {
        byte[] data = this.cloudEvent.getData().toBytes();
        return new String(data, StandardCharsets.UTF_8);
    }

    public String toString() {
        return "Topic: " + this.topic + ", Key: " + getKey() + ", Value: " + getValue();
    }

    public CloudEvent getCloudEvent() {
        return this.cloudEvent;
    }


}
