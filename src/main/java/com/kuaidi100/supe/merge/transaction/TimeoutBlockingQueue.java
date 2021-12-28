package com.kuaidi100.supe.merge.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FIFO
 * 超时
 * 阻塞
 *
 * @author GEYI
 * @date 2021年04月23日 11:24
 */
public class TimeoutBlockingQueue<E> {

    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private int putptr, takeptr, count;
    private int limit;
    // 单位纳秒
    private long timeout;

    private E[] items;

    public TimeoutBlockingQueue(E[] items) {
        this(items, 10, 5000);
    }

    /**
     * 创建一个超时阻塞队列
     *
     * @param items
     * @param limit
     * @param timeout 阻塞超时时间，单位毫秒
     * @return null
     * @author GEYI
     * @date 2021年12月08日 17:31
     */
    public TimeoutBlockingQueue(E[] items, int limit, long timeout) {
        this.items = items;
        this.limit = limit;
        this.timeout = timeout * 1000000;
    }

    /**
     * 获取当前队列中元素个数的一个大概值
     *
     * @return int
     * @author GEYI
     * @date 2021年04月28日 14:05
     */
    public int getCount() {
        return count;
    }

    public void put(E x) throws InterruptedException {
        lock.lock();
        try {
            while (count == items.length) {
                notFull.await();
            }
            items[putptr] = x;
            if (++putptr == items.length) {
                putptr = 0;
            }
            ++count;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public List<E> take() throws InterruptedException {
        lock.lock();
        try {
            long remaining = timeout;
            while (count < limit && remaining > 0) {
                remaining = notEmpty.awaitNanos(remaining);
            }

            int min = Math.min(count, limit);
            List<E> list = new ArrayList<>(min);
            for (int i = 0; i < min; i++) {
                E item = items[takeptr];
                if (item == null) {
                    break;
                }
                list.add(item);
                if (++takeptr == items.length) {
                    takeptr = 0;
                }
                --count;
            }
            notFull.signal();
            return list;
        } finally {
            lock.unlock();
        }
    }

    public List<E> poll() throws InterruptedException {
        lock.lock();
        try {
            while (count == 0) {
                notEmpty.await();
            }

            int min = Math.min(count, limit);
            List<E> list = new ArrayList<>(min);
            for (int i = 0; i < min; i++) {
                E item = items[takeptr];
                if (item == null) {
                    break;
                }
                list.add(item);
                if (++takeptr == items.length) {
                    takeptr = 0;
                }
                --count;
            }
            notFull.signal();
            return list;
        } finally {
            lock.unlock();
        }
    }

    private static class Task {
        int id;

        public Task(int id) {
            this.id = id;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 测试
        TimeoutBlockingQueue<Task> queue = new TimeoutBlockingQueue<>(new Task[10000], 500, 500);
        int limit = 512;
        Thread[] threads = new Thread[limit];
        for (int i = 0; i < limit; i++) {
            Thread putThread = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        queue.put(new Task(j));
                        System.out.println("put " + j);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[i] = putThread;
        }

        Thread takeThread = new Thread(() -> {
            try {
                int total = 0;
                while (true) {
                    List<Task> list = queue.poll();
                    total += list.size();
                    System.out.println("take " + list.size() + ", total " + total);
                    for (Task task : list) {
                        TimeUnit.MILLISECONDS.sleep(1);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        takeThread.start();
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        takeThread.join();
    }

}
