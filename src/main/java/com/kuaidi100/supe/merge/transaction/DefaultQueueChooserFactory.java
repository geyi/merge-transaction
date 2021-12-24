package com.kuaidi100.supe.merge.transaction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultQueueChooserFactory {
    public static final DefaultQueueChooserFactory INSTANCE = new DefaultQueueChooserFactory();

    private DefaultQueueChooserFactory() {}

    public QueueChooser newChooser(TimeoutBlockingQueue[] queues) {
        if (isPowerOfTwo(queues.length)) {
            return new PowerOfTwoQueueChooser(queues);
        } else {
            return new GenericQueueChooser(queues);
        }
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    private static final class PowerOfTwoQueueChooser implements QueueChooser {
        private final AtomicInteger idx = new AtomicInteger();
        private final TimeoutBlockingQueue[] queues;
        private final int i;

        PowerOfTwoQueueChooser(TimeoutBlockingQueue[] queues) {
            this.queues = queues;
            this.i = queues.length - 1;
        }

        @Override
        public TimeoutBlockingQueue next() {
            return queues[idx.getAndIncrement() & i];
        }
    }

    private static final class GenericQueueChooser implements QueueChooser {
        // Use a 'long' counter to avoid non-round-robin behaviour at the 32-bit overflow boundary.
        // The 64-bit long solves this by placing the overflow so far into the future, that no system
        // will encounter this in practice.
        private final AtomicLong idx = new AtomicLong();
        private final TimeoutBlockingQueue[] queues;

        GenericQueueChooser(TimeoutBlockingQueue[] queues) {
            this.queues = queues;
        }

        @Override
        public TimeoutBlockingQueue next() {
            return queues[(int) Math.abs(idx.getAndIncrement() % queues.length)];
        }
    }
}
