package com.kuaidi100.supe.merge.transaction.consumer;

import com.kuaidi100.supe.merge.transaction.CommonUtils;
import com.kuaidi100.supe.merge.transaction.Constant;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UpdateConsumer implements SqlHandler, Consumer<List<Package>> {

    private static final Logger log = LoggerFactory.getLogger(UpdateConsumer.class);

    private final ConcurrentHashMap<String, BoundSql> BOUND_SQL_CACHE = new ConcurrentHashMap<>();
    private String methodName;

    @Override
    public void sqlHandler(Method method) {
        this.methodName = method.getName();
        String sql = CommonUtils.getSql(method);
        Matcher matcher = Constant.SQL_PATTERN.matcher(sql);
        Class<?> parameterType = method.getParameterTypes()[0];
        List<SqlParameter> parameters = new ArrayList<>();
        while (matcher.find()) {
            String columnName = matcher.group(1);
            for (Field field : parameterType.getDeclaredFields()) {
                Column columnAnnotation = field.getAnnotation(Column.class);
                if (columnAnnotation == null || !columnAnnotation.value().equals(columnName)) {
                    continue;
                }
                SqlParameter sqlParameter = new SqlParameter();
                sqlParameter.setPrimitive(field.getType().isPrimitive());
                sqlParameter.setJavaType(field.getType());
                sqlParameter.setField(field);
                parameters.add(sqlParameter);
            }
        }
        log.info("{}|{}|{}", this.hashCode(), this.methodName, sql);
        BOUND_SQL_CACHE.put(this.methodName, new BoundSql(sql, parameters));
    }

    @Override
    public void accept(Connection connection, List<Package> list) {
        BoundSql boundSql = BOUND_SQL_CACHE.get(this.methodName);
        String sql = boundSql.getSql();
        List<SqlParameter> parameters = boundSql.getParameters();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        for (Package pkg : list) {
            Object arg = pkg.getBody().getArgs()[0];
            int i = 1;
            int count = 0;
            try {
                for (SqlParameter parameter : parameters) {
                    Field field = parameter.getField();
                    field.setAccessible(true);
                    ps.setObject(i++, field.get(arg));
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
                CommonUtils.retry(connection, Arrays.asList(pkg));
            }
            pkg.getBody().setResponse(count);
            CallbackHandler.run(pkg);
        }
    }

    public static void main(String[] args) {
        String s = "update t_order set frecmobile = ? where fid = ? and fuserid = ?";
        Pattern pattern = Pattern.compile("([a-zA-Z]+) = \\?");
        Matcher matcher = pattern.matcher(s);
        while (matcher.find()) {
            System.out.println(matcher.group(1));
        }
    }
}
