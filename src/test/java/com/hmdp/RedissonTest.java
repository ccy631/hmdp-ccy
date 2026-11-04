package com.hmdp;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedissonTest {
}

    @Test
    public void testRedissonLockAcquireAndRelease() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6379");

        RedissonClient redissonClient = Redisson.create(config);
        try {
            String lockName = "lock:test:" + System.currentTimeMillis();
            RLock lock = redissonClient.getLock(lockName);

            boolean acquired = lock.tryLock(3, 5, TimeUnit.SECONDS);
            assertTrue(acquired);
            assertTrue(lock.isHeldByCurrentThread());

            lock.unlock();
            assertFalse(lock.isHeldByCurrentThread());
        } finally {
            redissonClient.shutdown();
        }
    }
}
