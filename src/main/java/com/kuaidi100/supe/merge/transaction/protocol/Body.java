package com.kuaidi100.supe.merge.transaction.protocol;

import java.io.Serializable;
import java.lang.reflect.Type;

public class Body implements Serializable {
    private String serviceName;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] args;
    private Class<?> returnType;
    private Type genericReturnType;
    private Object response;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public Type getGenericReturnType() {
        return genericReturnType;
    }

    public void setGenericReturnType(Type genericReturnType) {
        this.genericReturnType = genericReturnType;
    }

    public Object getResponse() {
        return response;
    }

    public void setResponse(Object response) {
        this.response = response;
    }
}
