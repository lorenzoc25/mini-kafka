package main.producer;

import main.broker.Broker;
import main.data.Message;

public class Producer {
    private String topic;
    private Broker broker;

    // as for now, we assume that the main.producer will only produce to one topic
    // and that we do not need to specify properties for the main.producer
    public Producer(String topic) {
        this.topic = topic;
    }

    // in the future, we need to provide the address of the actual server like zookeeper
    // or KRaft controller for distributed storage and retention
    public void connectToBroker(Broker broker) {
        this.broker = broker;
    }
    
    public Boolean send(ProducerRecord record) {
        Message message = record.toMessage();
        this.broker.store(message);
        return true;
    }

}
