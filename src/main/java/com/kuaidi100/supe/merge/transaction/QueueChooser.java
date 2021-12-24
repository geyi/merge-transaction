package com.kuaidi100.supe.merge.transaction;

public interface QueueChooser {
    TimeoutBlockingQueue next();
}
