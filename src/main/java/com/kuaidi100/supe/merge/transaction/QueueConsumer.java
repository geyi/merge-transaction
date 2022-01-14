package com.kuaidi100.supe.merge.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class QueueConsumer<E> implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);

    private final TimeoutBlockingQueue<E> queue;
    private final Connection connection;
    private final String name;
    private final Consumer consumer;

    public QueueConsumer(TimeoutBlockingQueue<E> queue, Connection connection, Consumer<List<E>> consumer) {
        this(queue, connection, "queue-" + queue.hashCode(), consumer);
    }

    public QueueConsumer(TimeoutBlockingQueue<E> queue, Connection connection, String name,
                         Consumer<List<E>> consumer) {
        this.queue = queue;
        this.connection = connection;
        this.name = name;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<E> list = queue.take();
                if (list.isEmpty()) {
                    continue;
                }
                log.debug("{}|{}", name, list.size());

                consumer.accept(connection, list);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
