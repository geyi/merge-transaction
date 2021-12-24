package com.kuaidi100.supe.merge.transaction;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.util.List;

public class QueueGroup<E> {
    private TimeoutBlockingQueue<E>[] queues;
    private int queueNum;
    private QueueChooser queueChooser;

    public QueueGroup(int queueNum, Class<E> clazz, Consumer<List<E>> consumer) {
        this(queueNum, clazz, 1000, 50, 5, consumer);
    }

    public QueueGroup(int queueNum, Class<E> clazz, int queueSize, int limit, long timeout,
                      Consumer<List<E>> consumer) {
        this.queueNum = queueNum;
        this.queues = new TimeoutBlockingQueue[queueNum];
        for (int i = 0; i < queueNum; i++) {
            this.queues[i] = new TimeoutBlockingQueue<>((E[]) Array.newInstance(clazz, queueSize), limit, timeout);
            Connection connection = CommonUtils.getConnection("postgres", "postgres");
            new Thread(new QueueConsumer<>(this.queues[i], connection, consumer)).start();
        }

        queueChooser = DefaultQueueChooserFactory.INSTANCE.newChooser(queues);
    }

    public void put(E obj) throws InterruptedException {
        queueChooser.next().put(obj);
    }

    /**
     * least task
     *
     * @param obj
     * @author GEYI
     * @date 2021年12月05日 20:25
     */
    public void ltPut(E obj) throws InterruptedException {
        int min = queues[0].getCount();
        int idx = 0;
        for (int i = 1; i < queueNum; i++) {
            int count;
            if ((count = queues[i].getCount()) < min) {
                min = count;
                idx = i;
            }
        }
        queues[idx].put(obj);
    }
}
