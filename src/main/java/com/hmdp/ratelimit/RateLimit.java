package com.hmdp.ratelimit;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {
    String prefix() default "rate_limit:";

    /**
     * 时间窗口大小（秒）
     */
    int windowSeconds() default 60;

    /**
     * 窗口内最大允许请求数
     */
    int maxCount() default 10;

    LimitType limitType() default LimitType.USER;

    String message() default "操作过于频繁，请稍后再试！";
}