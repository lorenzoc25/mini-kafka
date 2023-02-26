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
    
    Boolean send(ProducerRecord record) {
        Message message = record.toMessage();
        this.broker.store(message);
        return true;
    }

}
