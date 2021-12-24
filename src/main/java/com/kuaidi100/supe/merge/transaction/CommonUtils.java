package com.kuaidi100.supe.merge.transaction;

import com.kuaidi100.supe.merge.transaction.annotation.Sql;
import com.kuaidi100.supe.merge.transaction.mapping.SqlDispatcher;
import com.kuaidi100.supe.merge.transaction.protocol.Body;
import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CommonUtils {

    private static final Logger log = LoggerFactory.getLogger(CommonUtils.class);

    public static Connection getConnection(String host, int port, String user, String pass) {
        try {
            String dbUrl = String.format(Constant.DB_URL, host, port);
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(dbUrl, user, pass);
        } catch (Exception e) {
            log.error("getConnect error: {}", e.getMessage());
            return null;
        }
    }

    public static Connection getConnection(String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(Constant.JDBC_URL, user, pass);
        } catch (Exception e) {
            log.error("getConnect error: {}", e.getMessage());
            return null;
        }
    }

    public static String getSql(Package pkg) {
        try {
            Body body = pkg.getBody();
            Class<?> clazz = Class.forName(body.getServiceName());
            Method method = clazz.getDeclaredMethod(body.getMethodName(), body.getParameterTypes());
            Sql sqlAnnotation = method.getAnnotation(Sql.class);
            return sqlAnnotation.sql();
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getSql(Method method) {
        Sql sqlAnnotation = method.getAnnotation(Sql.class);
        return sqlAnnotation.sql();
    }

    public static boolean isRecovery(Connection connection) {
        boolean isRecovery = false;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(Constant.PG_IS_IN_RECOVERY);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            isRecovery = resultSet.getBoolean(1);
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return isRecovery;
    }

    public static void retry(Connection connection, List<Package> packages) {
        /*if (isRecovery(connection)) {

        }*/
        try {
            TimeUnit.SECONDS.sleep(1);
            for (Package pkg : packages) {
                log.debug("retry {}", pkg.getHeader().getRequestId());
                String sn = String.format(Constant.QUEUE_NAME, pkg.getBody().getServiceName(),
                        pkg.getBody().getMethodName());
                SqlDispatcher.getInstance().get(sn).put(pkg);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
