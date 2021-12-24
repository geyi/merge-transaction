package com.kuaidi100.supe.merge.transaction.mapping;

import java.util.List;

public class BoundSql {
    private final String sql;
    private final List<SqlParameter> parameters;

    public BoundSql(String sql, List<SqlParameter> parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public String getSql() {
        return sql;
    }

    public List<SqlParameter> getParameters() {
        return parameters;
    }
}
