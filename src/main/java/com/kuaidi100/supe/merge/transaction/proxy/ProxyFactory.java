package com.kuaidi100.supe.merge.transaction.proxy;

import com.alibaba.fastjson.JSON;
import com.kuaidi100.supe.merge.transaction.Constant;
import com.kuaidi100.supe.merge.transaction.Consumer;
import com.kuaidi100.supe.merge.transaction.SqlHandler;
import com.kuaidi100.supe.merge.transaction.annotation.Sql;
import com.kuaidi100.supe.merge.transaction.mapping.CallbackHandler;
import com.kuaidi100.supe.merge.transaction.mapping.SqlDispatcher;
import com.kuaidi100.supe.merge.transaction.protocol.Body;
import com.kuaidi100.supe.merge.transaction.protocol.Header;
import com.kuaidi100.supe.merge.transaction.protocol.Package;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ProxyFactory {

    private static final Logger log = LoggerFactory.getLogger(ProxyFactory.class);

    public static <T> T getInstance(Class<T> interfaceInfo) throws IllegalAccessException, InstantiationException {
        ClassLoader classLoader = interfaceInfo.getClassLoader();
        Class<?>[] interfaces = {interfaceInfo};

        String sName = interfaceInfo.getName();
        Method[] methods = interfaceInfo.getMethods();
        for (Method method : methods) {
            Sql sqlAnnotation = method.getAnnotation(Sql.class);
            if (sqlAnnotation == null) {
                continue;
            }
            int queueNum = sqlAnnotation.queueNum();
            int queueSize = sqlAnnotation.queueSize();
            int limit = sqlAnnotation.limit();
            long timeout = sqlAnnotation.timeout();
            Class<? extends Consumer> consumer = sqlAnnotation.consumer();
            Consumer instance = consumer.newInstance();
            SqlDispatcher.getInstance().registry(String.format(Constant.QUEUE_NAME, sName, method.getName()),
                    queueNum, queueSize, limit, timeout, instance);
            ((SqlHandler) instance).sqlHandler(method);
        }

        return (T) Proxy.newProxyInstance(classLoader, interfaces, (proxy, method, args) -> {

            long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
            String serviceName = interfaceInfo.getName();
            String methodName = method.getName();
            Class<?>[] parameterTypes = method.getParameterTypes();
            Class<?> returnType = method.getReturnType();
            Type genericReturnType = method.getGenericReturnType();

            Header header = new Header();
            header.setRequestId(requestId);

            Body body = new Body();
            body.setServiceName(serviceName);
            body.setMethodName(methodName);
            body.setParameterTypes(parameterTypes);
            body.setArgs(args);
            body.setReturnType(returnType);
            body.setGenericReturnType(genericReturnType);

            CompletableFuture<Object> completableFuture = new CompletableFuture<>();
            CallbackHandler.add(requestId, completableFuture);
            /*CompletableFuture cf = CallbackHandler.add(requestId, completableFuture);
            if (cf != null) {
                log.debug("cf is not null");
            }*/

            log.debug("req pkg,{},{},{}", requestId, methodName, JSON.toJSONString(args));

            SqlDispatcher.getInstance().get(String.format(Constant.QUEUE_NAME, serviceName, methodName))
                    .put(new Package(header, body));

            /*Object o = completableFuture.get(5, TimeUnit.SECONDS);
            if (o == null) {
                log.debug("o is null,{},{}", requestId, methodName, JSON.toJSONString(args));
            }
            return o;*/
            return completableFuture.get();
        });
    }

    private static Object returnHandler(Class<?> returnType) throws IllegalAccessException, InstantiationException {
        if (returnType.isPrimitive()) {
            String name = returnType.getName();
            if ("int".equals(name)) {
                return 0;
            }
        } else if (returnType.equals(List.class)) {
            return new ArrayList<>(0);
        } else {
            return returnType.newInstance();
        }
        return 0;
    }

}
