package com.kuaidi100.supe.merge.transaction.mapping;

import java.lang.reflect.Field;

public class SqlParameter {
    private Boolean isPrimitive;
    private Class<?> javaType;
    private Field field;

    public Boolean getPrimitive() {
        return isPrimitive;
    }

    public void setPrimitive(Boolean primitive) {
        isPrimitive = primitive;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public void setJavaType(Class<?> javaType) {
        this.javaType = javaType;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }
}
