package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Receiver, BlockingQueue<String>>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        data.putIfAbsent(receiver.name(), new ConcurrentHashMap<>());
        data.get(receiver.name()).put(receiver, new LinkedBlockingQueue<>());
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new ConcurrentHashMap<>());
        data.get(message.name()).forEach((receiver, queue) -> {
            queue.add(message.text());
            condition.on();
        });
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var topicKey : receivers.keySet()) {
                var receiverQueues = data.getOrDefault(topicKey, new ConcurrentHashMap<>());
                receiverQueues.forEach((receiver, queue) -> {
                    String message;
                    while ((message = queue.poll()) != null) {
                        receiver.receive(message);
                    }
                });
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


