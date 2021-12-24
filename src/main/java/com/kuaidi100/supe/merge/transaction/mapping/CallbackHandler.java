package com.kuaidi100.supe.merge.transaction.mapping;

import com.kuaidi100.supe.merge.transaction.protocol.Package;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class CallbackHandler {

    private static ConcurrentHashMap<Long, CompletableFuture<Object>> mapping = new ConcurrentHashMap<>(2048);

    public static void add(Long requestId, CompletableFuture cf) {
        mapping.putIfAbsent(requestId, cf);
    }

    public static void remove(Long requestId) {
        mapping.remove(requestId);
    }

    public static void run(Package pkg) {
        mapping.remove(pkg.getHeader().getRequestId()).complete(pkg.getBody().getResponse());
    }

}
