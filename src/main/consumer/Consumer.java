package main.consumer;

import main.broker.Broker;
import main.data.Message;
import java.util.List;
import main.consumer.ConsumerRecord;

public interface Consumer {
    // can be either single main.consumer or a main.consumer group

    public void connect(Broker broker);

    public void subscribe(String topic);
    public void subscribe(List<String> topics);
    public void unsubscribe(String topic);
    public List<ConsumerRecord> poll();
    public List<String> getTopics();
    public Integer getOffsetForTopic(String topic);
    public Boolean commitOffsetForTopic(String topic, Integer offset);
}

