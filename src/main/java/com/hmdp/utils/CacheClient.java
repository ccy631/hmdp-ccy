package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.service.impl.ShopServiceImpl.CACHE_REBUILDE_EXECUTOR;
import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(String keyprefix,ID id, Class<R> type, Function<ID, R> dbFallback,Long time, TimeUnit unit) {
        String key = CACHE_SHOP_KEY + id;

        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(JSONUtil.toJsonStr(dbFallback.apply(id)), type);
        }

        if(shopJson == null){
            return null;
        }

        //不存在，根据id查数据库
        R r = dbFallback.apply(id);

        //不存在，返回错误
        if(r == null){
            //将空值写入数据库
            stringRedisTemplate.opsForValue().set(key,"" ,CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //存在
        this.set(key, r, time,unit);

        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.存在，直接返回
            return null;
        }

        //4.命中，需要先把json反序列化为对象
        RedisData redisdata = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(redisdata), type);
        LocalDateTime expireTime = redisdata.getExpireTime();

        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期
            return r;
        }
        //已过期，需要缓存重建
        //6.实现缓存重建
        //6.1获取互斥锁
        String lockKey = CACHE_SHOP_KEY + id;
        boolean islock = tryLock(lockKey);
        //6.2判断是否获取锁成功
        if(islock){
            CACHE_REBUILDE_EXECUTOR.submit(() -> {
                try {
                    // 查数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
