package producer;

import broker.Broker;
import data.Message;

public class Producer {
    private String topic;
    private Broker broker;

    // as for now, we assume that the producer will only produce to one topic
    // and that we do not need to specify properties for the producer
    public Producer(String topic) {
        this.topic = topic;
    }

    // in the future, we need to provide the address of the actual server like zookeeper
    // or KRaft controller for distributed storage and retention
    public void conncetToBroker(Broker broker) {
        this.broker = broker;
    }
    
    Boolean send(ProducerRecord record) {
        Message message = record.toMessage();
        this.broker.store(message);
        return true;
    }

}
