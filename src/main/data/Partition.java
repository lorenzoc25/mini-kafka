package main.data;

import java.util.List;

public class Partition {
    private final Integer partitionId;
    private List<Message> messages;
    /**
     * A partition is a list of messages, should be managed by the
     * broker only
     * @param partitionId
     */
    public Partition(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public void addMessage(Message message) {
        this.messages.add(message);
    }

    public void addMessage(List<Message> messages) {
        this.messages.addAll(messages);
    }

    public Integer getPartitionId() {
        return this.partitionId;
    }

    public List<Message> getMessages() {
        return this.messages;
    }
}
