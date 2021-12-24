package com.kuaidi100.supe.merge.transaction.mapping;

import com.kuaidi100.supe.merge.transaction.Consumer;
import com.kuaidi100.supe.merge.transaction.QueueGroup;
import com.kuaidi100.supe.merge.transaction.protocol.Package;

import java.util.HashMap;
import java.util.Map;

public class SqlDispatcher {

    private static SqlDispatcher instance = new SqlDispatcher();
    private static Map<String, QueueGroup> mapping = new HashMap<>();

    private SqlDispatcher() {}

    public static SqlDispatcher getInstance() {
        return instance;
    }

    public void registry(String serviceName, int queueNum, int queueSize, int limit, long timeout, Consumer consumer) {
        if (!mapping.containsKey(serviceName)) {
            synchronized (SqlDispatcher.class) {
                if (!mapping.containsKey(serviceName)) {
                    mapping.put(serviceName, new QueueGroup<>(queueNum, Package.class, queueSize, limit, timeout,
                            consumer));
                }
            }
        }

    }

    public QueueGroup get(String serviceName) {
        return mapping.get(serviceName);
    }

}
