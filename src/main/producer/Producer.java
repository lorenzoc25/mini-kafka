package main.producer;

import main.broker.Broker;
import main.data.Message;

public class Producer {
    private Broker broker;

    /**
     * For now, producer are initialized by calling the `connect` method
     * no other functionality is provided, so no other parameters are needed
     * @param None
     */
    public Producer() {
    }

    // in the future, we need to provide the address of the actual server like zookeeper
    // or KRaft controller for distributed storage and retention

    public void connect(Broker broker) {
        this.broker = broker;
    }
    
    public Boolean send(ProducerRecord record) {
        this.broker.store(record);
        return true;
    }

}
