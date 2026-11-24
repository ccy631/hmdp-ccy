package com.hmdp.ratelimit;

import com.hmdp.dto.Result;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.Collections;

@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // Lua脚本返回1表示放行，0表示拒绝
    private DefaultRedisScript<Long> rateLimitScript;

    @PostConstruct
    public void init() {
        rateLimitScript = new DefaultRedisScript<>();
        // 滑动窗口限流脚本：在Redis原子执行，避免并发竞争
        rateLimitScript.setLocation(new ClassPathResource("sliding_window_rate_limit.lua"));
        rateLimitScript.setResultType(Long.class);
    }

    @Around("@annotation(com.hmdp.ratelimit.RateLimit)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        RateLimit rateLimit = method.getAnnotation(RateLimit.class);

        String bizKey = buildBizKey(method, rateLimit.limitType());
        // key维度示例：rl:user:Controller.method:1001
        String redisKey = rateLimit.prefix() + rateLimit.limitType().name().toLowerCase() + ":" + bizKey;

        long nowMillis = System.currentTimeMillis();
        long windowMillis = rateLimit.windowSeconds() * 1000L;

        Long allowed = stringRedisTemplate.execute(
                rateLimitScript,
                Collections.singletonList(redisKey),
                String.valueOf(nowMillis),
                String.valueOf(windowMillis),
                String.valueOf(rateLimit.maxCount())
        );

        if (allowed == null || allowed == 0L) {
            log.warn("触发限流，key={}", redisKey);
            return Result.fail(rateLimit.message());
        }

        // 限流通过，继续执行原方法
        return joinPoint.proceed();
    }

    private String buildBizKey(Method method, LimitType limitType) {
        String methodName = method.getDeclaringClass().getSimpleName() + "." + method.getName();
        switch (limitType) {
            case USER:
                Long userId = UserHolder.getUser() != null ? UserHolder.getUser().getId() : null;
                // 未登录用户归并到guest，避免空用户导致key不稳定
                return methodName + ":" + (userId != null ? userId : "guest");
            case IP:
                return methodName + ":" + getClientIp();
            case GLOBAL:
            default:
                // 全局维度：同一接口共享一个限流桶
                return methodName;
        }
    }

    private String getClientIp() {
        ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attrs == null) {
            return "unknown";
        }
        HttpServletRequest request = attrs.getRequest();
        // 优先取代理透传IP，再回退到直连IP
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            // X-Forwarded-For可能是IP链，取第一个真实来源IP
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }
}