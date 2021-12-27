package com.kuaidi100.supe.merge.transaction.consumer;

import com.kuaidi100.supe.merge.transaction.CommonUtils;
import com.kuaidi100.supe.merge.transaction.Consumer;
import com.kuaidi100.supe.merge.transaction.SqlHandler;
import com.kuaidi100.supe.merge.transaction.mapping.BoundSql;
import com.kuaidi100.supe.merge.transaction.mapping.CallbackHandler;
import com.kuaidi100.supe.merge.transaction.mapping.SqlParameter;
import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DeleteByColumnConsumer implements SqlHandler, Consumer<List<Package>> {

    private static final Logger log = LoggerFactory.getLogger(DeleteByColumnConsumer.class);

    private final ConcurrentHashMap<String, BoundSql> BOUND_SQL_CACHE = new ConcurrentHashMap<>();
    private String methodName;

    @Override
    public void sqlHandler(Method method) {
        this.methodName = method.getName();
        String sql = CommonUtils.getSql(method);
        Class<?>[] parameterTypes = method.getParameterTypes();
        List<SqlParameter> parameters = new ArrayList<>();
        for (Class<?> parameterType : parameterTypes) {
            SqlParameter sqlParameter = new SqlParameter();
            sqlParameter.setPrimitive(parameterType.isPrimitive());
            sqlParameter.setJavaType(parameterType);
            parameters.add(sqlParameter);
        }
        log.info("{}|{}|{}", this.hashCode(), this.methodName, sql);
        BOUND_SQL_CACHE.put(this.methodName, new BoundSql(sql, parameters));
    }

    @Override
    public void accept(Connection connection, List<Package> list) {
        BoundSql boundSql = BOUND_SQL_CACHE.get(this.methodName);
        String sql = boundSql.getSql();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        for (Package pkg : list) {
            Object[] args = pkg.getBody().getArgs();
            int i = 1;
            int count = 0;
            try {
                for (Object arg : args) {
                    ps.setObject(i++, arg);
                }
                connection.setAutoCommit(false);
                count = ps.executeUpdate();
                connection.commit();
                ps.clearParameters();
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
            pkg.getBody().setResponse(count);
            CallbackHandler.run(pkg);
        }
    }
}
