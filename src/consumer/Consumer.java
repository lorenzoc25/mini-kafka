package consumer;

public interface Consumer {
    // can be either single consumer or a consumer group
    public void subscribe(String topic);
    public void unsubscribe(String topic);
    public void poll();

}

