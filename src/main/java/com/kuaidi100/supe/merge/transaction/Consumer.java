package com.kuaidi100.supe.merge.transaction;

import java.sql.Connection;

@FunctionalInterface
public interface Consumer<T> {
    void accept(Connection connection, T t);
}
