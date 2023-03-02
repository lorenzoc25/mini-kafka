// TO BE IMPLEMENTED LATER

//package main.consumer;
//
//import main.broker.Broker;
//
//import java.util.List;
//import java.util.ArrayList;
//import main.data.Message;
//
//public class ConsumerGroup implements  Consumer {
//    private List<Consumer> consumers;
//    private String groupId;
//
//    /**
//     * Create a consumer group with a given group id
//     * Note that no consumer is added to the group at creation,
//     * need to use `addConsumer` to add a consumer to the group
//     * @param {string} groupId - the group id of the consumer group
//     */
//    public ConsumerGroup(String groupId) {
//        this.groupId = groupId;
//        this.consumers = new ArrayList<>();
//    }
//
//    public void addConsumer(Consumer consumer) {
//        this.consumers.add(consumer);
//    }
//
//    public void connect(Broker broker) {
//        for (Consumer consumer : this.consumers) {
//            consumer.connect(broker);
//        }
//    }
//
//
//    public void subscribe(String topic) {
//        for (Consumer consumer : this.consumers) {
//            consumer.subscribe(topic);
//        }
//    }
//
//    public void subscribe(List<String> topics) {
//        for (Consumer consumer : this.consumers) {
//            consumer.subscribe(topics);
//        }
//    }
//
//    public void unsubscribe(String topic) {
//        for (Consumer consumer : this.consumers) {
//            consumer.unsubscribe(topic);
//        }
//    }
//
//    public List<String> getTopics() {
//        List<String> topics = new ArrayList<>();
//        for (Consumer consumer : this.consumers) {
//            topics.addAll(consumer.getTopics());
//        }
//        return topics;
//    }
//
//    public List<ConsumerRecord> poll() {
//        List<ConsumerRecord> messages = new ArrayList<>();
//        for (Consumer consumer : this.consumers) {
//            messages.addAll(consumer.poll());
//        }
//        return messages;
//    }
//    // PLACE FILLER, DOES NOT WORK
//
//    public Integer getOffsetForTopic(String topic) {
//        for (Consumer consumer : this.consumers) {
//            if (consumer.getTopics().contains(topic)) {
//                return consumer.getOffsetForTopic(topic);
//            }
//        }
//        return null;
//    }
//    // DOES NOT WORK
//    public Boolean commitOffsetForTopic(String topic, Integer offset) {
//        for (Consumer consumer : this.consumers) {
//            if (consumer.getTopics().contains(topic)) {
//                return consumer.commitOffsetForTopic(topic, offset);
//            }
//        }
//        return false;
//    }
//}
