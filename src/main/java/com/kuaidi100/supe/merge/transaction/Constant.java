package com.kuaidi100.supe.merge.transaction;

import java.util.regex.Pattern;

public class Constant {
    // 192.168.10.128
    public static final String DB_HOST = "localhost";
    public static final String DB_URL = "jdbc:postgresql://%s:%d/test";
    public static final String QUEUE_NAME = "%s@%s";
    public static final String PG_IS_IN_RECOVERY = "select pg_is_in_recovery();";
    public static final String JDBC_URL = System.getProperty("db.url");
    public static final Pattern SQL_PATTERN = Pattern.compile("([a-zA-Z]+) = \\?");
}
