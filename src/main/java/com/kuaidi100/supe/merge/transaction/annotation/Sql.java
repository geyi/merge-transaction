package com.kuaidi100.supe.merge.transaction.annotation;

import com.kuaidi100.supe.merge.transaction.Consumer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Sql {
    String sql() default "";

    int queueNum() default 1;

    int queueSize() default 100;

    int limit() default 10;

    long timeout() default 10;

    Class<? extends Consumer> consumer();
}
