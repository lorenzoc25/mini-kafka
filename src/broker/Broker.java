package broker;

import data.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class Broker {
    private Map<String, List<Message>> records;
    public Broker() {
        this.records = new HashMap<>();
    }

    public void store(Message message) {
        String topic = message.getTopic();
        if (this.records.containsKey(topic)) {
            this.records.get(topic).add(message);
        } else {
            this.records.put(topic, new ArrayList<Message>());
            this.records.get(topic).add(message);
        }
    }

    public Message get(String topic, String key) {
        if (this.records.containsKey(topic)) {
            for (Message message : this.records.get(topic)) {
                if (message.getKey().equals(key)) {
                    return message;
                }
            }
        }
        return null;
    }

}
