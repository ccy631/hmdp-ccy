package com.hmdp.task;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class OrderTimeoutTask {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    @Lazy // 加上 @Lazy 解决循环依赖
    private OrderTimeoutTask selfProxy;

    /**
     * 每分钟执行一次，扫描超过 15 分钟未支付的秒杀订单
     */
    @Scheduled(cron = "0 */1 * * * ?")
    public void cancelTimeoutOrders() {
        // 1. 加分布式锁，避免集群多节点同时扫描同一批超时订单
        RLock lock = redissonClient.getLock("lock:task:cancel_order");
        boolean isLock = false;
        try {
            // 尝试获取锁，不等待（waitTime=0），锁自动释放时间30秒
            isLock = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!isLock) {
                return;
            }

            // 2. 查询创建时间在 15 分钟前，且状态仍为未支付（status = 1）的订单
            LocalDateTime expireTime = LocalDateTime.now().minusMinutes(15);
            List<VoucherOrder> timeoutOrders = voucherOrderService.lambdaQuery()
                    .eq(VoucherOrder::getStatus, 1)
                    .lt(VoucherOrder::getCreateTime, expireTime)
                    .last("LIMIT 100") // 每次最多捞100条，防止一次性全表扫描打爆内存
                    .list();

            if (timeoutOrders.isEmpty()) {
                return;
            }

            log.info("扫描到 {} 条超时未支付订单，开始执行关单...", timeoutOrders.size());
            for (VoucherOrder order : timeoutOrders) {
                try {
                    // 抽离单笔订单处理方法，保证单笔事务隔离，互不影响
                    selfProxy.processSingleTimeoutOrder(order);
                } catch (Exception e) {
                    log.error("处理超时订单失败，orderId: {}", order.getId(), e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (isLock && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 单笔订单关单与库存回滚（需确保被代理调用以生效事务，实际开发建议注入自身代理对象或拆出独立Service）
     */
    @Transactional(rollbackFor = Exception.class)
    public void processSingleTimeoutOrder(VoucherOrder order) {
        // 1. 核心：乐观锁修改订单状态 1(未支付) -> 4(已取消)
        boolean cancelled = voucherOrderService.update()
                .set("status", 4)
                .set("update_time", LocalDateTime.now())
                .eq("id", order.getId())
                .eq("status", 1) // 乐观锁：若此时用户刚好完成支付(status=2)，此处更新返回false，直接跳过回滚！
                .update();

        if (!cancelled) {
            log.info("订单 {} 状态已变更（可能已完成支付），跳过关单", order.getId());
            return;
        }

        // 2. 回滚 MySQL 秒杀券库存 (+1)
        Long voucherId = order.getVoucherId();
        boolean stockRecovered = seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", voucherId)
                .update();

        if (!stockRecovered) {
            throw new RuntimeException("回滚数据库库存失败，触发事务回滚");
        }

        // 3. 回滚 Redis 缓存中的库存和一人一单集合（注意：Key前缀需与你seckill.lua中的保持完全一致）
        String stockKey = "seckill:stock:" + voucherId;
        String orderSetKey = "seckill:order:" + voucherId;
        
        stringRedisTemplate.opsForValue().increment(stockKey);
        stringRedisTemplate.opsForSet().remove(orderSetKey, order.getUserId().toString());
        
        log.info("订单 {} 超时自动关闭成功，MySQL与Redis库存已回滚", order.getId());
    }
}