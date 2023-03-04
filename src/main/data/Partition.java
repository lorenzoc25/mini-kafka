package main.data;

import java.util.List;
import java.util.ArrayList;

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
        this.messages = new ArrayList<>();
    }

    public void addMessage(Message message) {
        this.messages.add(message);
    }

    public void addMessage(List<Message> messages) {
        this.messages.addAll(messages);
    }

    public void updateMessageForOffset(Integer offset) {
        this.messages = this.messages.subList(offset, this.messages.size());
    }

    public Integer getPartitionId() {
        return this.partitionId;
    }

    public List<Message> getMessages() {
        return this.messages;
    }
}
