package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


/**
 * 基于 Redis 的分布式全局唯一 ID 生成器
 *
 * 核心特性：
 * 1. 全局唯一性：借助 Redis 原子自增保证 ID 不重复
 * 2. 高可用与高性能：基于内存自增，支持高并发生成
 * 3. 趋势递增：高位为时间戳，保证整体有序递增，利于 B+ 树索引存储
 * 4. 信息安全性：不暴露真实业务连续自增规律
 *
 * ID 结构（64位 Long）：
 * 0 (1位符号位) | 时间戳 (31位，相对基准时间秒数) | 自增序列号 (32位，Redis每秒自增计数)
 */


@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP = 1640995200L;
    /*
    序列号的位数
     */
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        //2.生成序列号
        //2.1获取当前日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2实现自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }
}
