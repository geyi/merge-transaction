package com.kuaidi100.supe.merge.transaction.consumer;

import com.kuaidi100.supe.merge.transaction.CommonUtils;
import com.kuaidi100.supe.merge.transaction.Consumer;
import com.kuaidi100.supe.merge.transaction.SqlHandler;
import com.kuaidi100.supe.merge.transaction.annotation.Column;
import com.kuaidi100.supe.merge.transaction.mapping.BoundSql;
import com.kuaidi100.supe.merge.transaction.mapping.CallbackHandler;
import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FindByColumnConsumer implements SqlHandler, Consumer<List<Package>> {

    private static final Logger log = LoggerFactory.getLogger(FindByColumnConsumer.class);

    private final ConcurrentHashMap<String, BoundSql> BOUND_SQL_CACHE = new ConcurrentHashMap<>();
    private String methodName;

    @Override
    public void sqlHandler(Method method) {
        this.methodName = method.getName();
        String sql = CommonUtils.getSql(method);
        log.info("{}|{}|{}", this.hashCode(), this.methodName, sql);
        BOUND_SQL_CACHE.put(this.methodName, new BoundSql(sql, null));
    }

    @Override
    public void accept(Connection connection, List<Package> list) {
        int limit = list.size();
        String sql = BOUND_SQL_CACHE.get(this.methodName).getSql();

        StringBuilder sqlBuilder = new StringBuilder((sql.length() + 1) * limit);
        for (int i = 0; i < limit; i++) {
            sqlBuilder.append(sql).append(";");
        }

        PreparedStatement ps;
        try {
            ps = connection.prepareStatement(sqlBuilder.toString());
            for (int i = 0; i < limit; i++) {
                Object[] args = list.get(i).getBody().getArgs();
                for (int j = 0, limitJ = args.length; j < limitJ; j++) {
                    Object arg = args[j];
                    ps.setObject(i * limitJ + (j + 1), arg);
                }
            }
            ps.execute();
            ResultSet rs = ps.getResultSet();
            int j = 0;
            List<Package> removeList = new ArrayList<>(list.size());
            while (rs.next()) {
                Package pkg = list.get(j);
                removeList.add(pkg);
                response(rs, pkg);
            }
            j++;
            while (ps.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                rs = ps.getResultSet();
                while (rs.next()) {
                    Package pkg = list.get(j);
                    removeList.add(pkg);
                    response(rs, list.get(j));
                }
                j++;
            }
            list.removeAll(removeList);
            if (!list.isEmpty()) {
                for (Package pkg : list) {
                    sendResp(null, pkg);
                }
            }
        } catch (Exception sqlException) {
            sqlException.printStackTrace();
        }
    }

    private void response(ResultSet rs, Package pkg) throws SQLException, IllegalAccessException, InstantiationException {
        Class<?> returnType = pkg.getBody().getReturnType();
        Class<?> genericType = null;
        if (returnType.equals(List.class)) {
            Type genericReturnType = pkg.getBody().getGenericReturnType();
            if (genericReturnType instanceof ParameterizedType) {
                Type[] actualTypeArguments = ((ParameterizedType) genericReturnType).getActualTypeArguments();
                genericType = (Class) actualTypeArguments[0];
            }
            List<Object> list = new ArrayList<>();
            do {
                list.add(buildObj(genericType, rs));
            } while (rs.next());
            sendResp(list, pkg);
        } else {
            sendResp(buildObj(returnType, rs), pkg);
        }
    }

    private Object buildObj(Class<?> returnType, ResultSet rs) throws SQLException, IllegalAccessException,
            InstantiationException {
        Object ret = returnType.newInstance();
        for (Field field : returnType.getDeclaredFields()) {
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation == null) {
                continue;
            }
            String column = columnAnnotation.value();
            Object columnVal = rs.getObject(column, field.getType());
            if (columnVal == null) {
                continue;
            }
            field.setAccessible(true);
            field.set(ret, columnVal);
        }
        return ret;
    }

    private void sendResp(Object ret, Package pkg) {
        pkg.getBody().setResponse(ret);
        CallbackHandler.run(pkg);
    }
}
