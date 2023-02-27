package consumer;

import broker.Broker;

public class ConsumerImpl implements Consumer {
    private String topic;
    private final Integer consumerId;
    private Broker broker;

    public ConsumerImpl(String topic, String key) {
        this.topic = topic;
        this.consumerId = 0;
    }

    // like the producer, this is only a primitive implementation
    public void connectToBroker(Broker broker) {
        this.broker = broker;
    }

    public void subscribe(String topic) {
        this.broker.addSubscription(topic, this.consumerId);
    }

    public void unsubscribe(String topic) {
        this.broker.removeSubscription(topic, this.consumerId);
    }

    public void poll() {
    }
}
