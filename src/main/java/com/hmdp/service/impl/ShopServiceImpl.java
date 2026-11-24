package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 服务实现类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    // 1. 注入 Caffeine 本地缓存（对应 CaffeineConfig 中的 shopLocalCache）
    @Resource
    private Cache<Long, Shop> shopLocalCache;

    // 2. 注入 KafkaTemplate 用于删缓存失败时的补偿重试
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String CACHE_DELETE_TOPIC = "shop-cache-delete-topic";

    @Override
    public Result queryById(Long id) {
        // 1. 先查 Caffeine 本地缓存（命中直接返回，减少网络 IO）
        Shop shop = shopLocalCache.getIfPresent(id);
        if (shop != null) {
            return Result.ok(shop);
        }

        // 2. 本地未命中，走 CacheClient（Redis 逻辑过期/空值缓存 + DB 兜底）
        shop = cacheClient.queryWithLogicalExpire(
                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }

        // 3. 回填 Caffeine 本地缓存
        shopLocalCache.put(id, shop);
        return Result.ok(shop);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1. 查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2. 封装逻辑过期时间
        RedisData redisdata = new RedisData();
        redisdata.setData(shop);
        redisdata.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 3. 写入 redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisdata));
        // 同步刷新本地缓存，避免本地缓存还是旧数据
        if (shop != null) {
            shopLocalCache.put(id, shop);
        }
    }

    public static final ExecutorService CACHE_REBUILDE_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. 从 redis 查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 3. 不存在，直接返回 null
            return null;
        }

        // 4. 命中，需要先把 json 反序列化为对象
        RedisData redisdata = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisdata.getData(), Shop.class);
        LocalDateTime expireTime = redisdata.getExpireTime();

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期
            return shop;
        }
        // 已过期，需要缓存重建
        // 6. 实现缓存重建
        // 6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean islock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (islock) {
            CACHE_REBUILDE_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return shop;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        // 1. 更新数据库
        updateById(shop);

        // 2. 失效本地缓存
        shopLocalCache.invalidate(id);

        // 3. 删除 Redis 缓存；异常时发送 Kafka 消息进行异步重试补偿
        String cacheKey = CACHE_SHOP_KEY + id;
        try {
            stringRedisTemplate.delete(cacheKey);
        } catch (Exception e) {
            log.warn("删除 Redis 缓存失败，投递 Kafka 补偿消息，shopId={}", id, e);
            kafkaTemplate.send(CACHE_DELETE_TOPIC, String.valueOf(id));
        }

        return Result.ok();
    }
}