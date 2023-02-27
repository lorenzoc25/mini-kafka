package main.consumer;

import main.broker.Broker;
import main.data.Message;
import java.util.List;

public interface Consumer {
    // can be either single main.consumer or a main.consumer group
    public void connectToBroker(Broker broker);
    public void subscribe(String topic);
    public void unsubscribe(String topic);
    public List<Message> poll();



}

