package com.kuaidi100.supe.merge.transaction.consumer;

import com.kuaidi100.supe.merge.transaction.CommonUtils;
import com.kuaidi100.supe.merge.transaction.Consumer;
import com.kuaidi100.supe.merge.transaction.SqlHandler;
import com.kuaidi100.supe.merge.transaction.annotation.Column;
import com.kuaidi100.supe.merge.transaction.mapping.BoundSql;
import com.kuaidi100.supe.merge.transaction.mapping.CallbackHandler;
import com.kuaidi100.supe.merge.transaction.mapping.SqlParameter;
import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BatchInsertConsumer implements SqlHandler, Consumer<List<Package>> {

    private static final Logger log = LoggerFactory.getLogger(BatchInsertConsumer.class);

    private final ConcurrentHashMap<String, BoundSql> BOUND_SQL_CACHE = new ConcurrentHashMap<>();
    private String methodName;

    @Override
    public void sqlHandler(Method method) {
        this.methodName = method.getName();
        String sql = CommonUtils.getSql(method);
        Class<?> parameterType = method.getParameterTypes()[0];
        StringBuilder sqlBuilder = new StringBuilder(sql).append("(");
        StringBuilder paramBuilder = new StringBuilder();
        List<SqlParameter> parameters = new ArrayList<>();
        for (Field field : parameterType.getDeclaredFields()) {
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation == null || columnAnnotation.pk()) {
                continue;
            }
            sqlBuilder.append(columnAnnotation.value()).append(",");
            paramBuilder.append("?,");

            SqlParameter sqlParameter = new SqlParameter();
            sqlParameter.setPrimitive(field.getType().isPrimitive());
            sqlParameter.setJavaType(field.getType());
            sqlParameter.setField(field);
            parameters.add(sqlParameter);
        }
        sqlBuilder.setLength(sqlBuilder.length() - 1);
        paramBuilder.setLength(paramBuilder.length() - 1);
        sqlBuilder.append(") values (").append(paramBuilder).append(")");
        log.info("{}|{}|{}", this.hashCode(), this.methodName, sqlBuilder.toString());
        BOUND_SQL_CACHE.put(this.methodName, new BoundSql(sqlBuilder.toString(), parameters));
    }

    @Override
    public void accept(Connection connection, List<Package> list) {
        BoundSql boundSql = BOUND_SQL_CACHE.get(this.methodName);
        String sql = boundSql.getSql();
        List<SqlParameter> parameters = boundSql.getParameters();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (Package pkg : list) {
                Object arg = pkg.getBody().getArgs()[0];
                int i = 1;
                for (SqlParameter parameter : parameters) {
                    Field field = parameter.getField();
                    field.setAccessible(true);
                    ps.setObject(i++, field.get(arg));
                }
                ps.addBatch();
            }
            connection.setAutoCommit(false);
            ps.executeBatch();
            connection.commit();
            ps.clearBatch();
        } catch (Exception sqlException) {
            sqlException.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            CommonUtils.retry(connection, list);
        }
        try {
            ResultSet rs = ps.getGeneratedKeys();
            int j = 0;
            while (rs.next()) {
                Package pkg = list.get(j++);
                pkg.getBody().setResponse(rs.getInt("fid"));
                CallbackHandler.run(pkg);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
