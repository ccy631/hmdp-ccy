package com.hmdp.ratelimit;

public enum LimitType {
    GLOBAL, // 全局限流
    IP,     // 按客户端 IP 限流
    USER    // 按登录用户 ID 限流
}