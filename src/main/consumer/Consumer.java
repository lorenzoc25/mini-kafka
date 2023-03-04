package main.consumer;

import main.broker.Broker;

import java.util.List;

public interface Consumer {
    // can be either single main.consumer or a main.consumer group
    void connect(Broker broker);

    void subscribe(String topic);

    void subscribe(List<String> topics);

    void unsubscribe(String topic);

    List<ConsumerRecord> poll();

    List<String> getTopics();

    int getOffsetFor(String topic, Integer partitionId);

    Boolean commitOffsetFor(String topic, Integer partitionId, Integer offset);
}

