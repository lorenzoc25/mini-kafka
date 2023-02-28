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
    public Integer getOffset(String topic);
    /**
     * TODO: handle commit offset on the consumer end. Currently have broker method
     *       for commit offset but each consumer needs to keep track of its own offset
     *       as well. If those two offsets are not the same, the min value between
     *       those two offsets will be used as the offset to commit to the broker.
     *       And the broker would clean up all the messages before that offset.
     */
}

