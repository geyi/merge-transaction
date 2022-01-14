package com.kuaidi100.supe.merge.transaction.mapping;

import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class CallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(CallbackHandler.class);

    private static ConcurrentHashMap<Long, CompletableFuture<Object>> mapping = new ConcurrentHashMap<>(65535);

    public static CompletableFuture add(Long requestId, CompletableFuture cf) {
        return mapping.putIfAbsent(requestId, cf);
    }

    public static void remove(Long requestId) {
        mapping.remove(requestId);
    }

    public static void run(Package pkg) {
        mapping.remove(pkg.getHeader().getRequestId()).complete(pkg.getBody().getResponse());
        /*CompletableFuture<Object> remove = mapping.remove(pkg.getHeader().getRequestId());
        if (remove != null) {
            remove.complete(pkg.getBody().getResponse());
        }*/
    }

}
