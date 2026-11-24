package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;

@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 【关键修复1】自己注入自己的代理对象（加上 @Lazy 防止循环依赖）
     * 替代原有的 AopContext.currentProxy()，避免服务重启消费积压消息时 proxy 为 null 报空指针！
     */
    @Lazy
    @Resource
    private IVoucherOrderService proxy;

    private static final String TOPIC_SECKILL_ORDERS = "seckill-orders";
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // 【关键修复2】指向不包含 xadd stream.orders 的新 Lua 脚本
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill_advance.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 1. 秒杀抢购主入口（生产者）
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 1. 执行 Lua 脚本：校验库存 + 校验一人一单 + 预扣 Redis 库存
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r ==1 ? "库存不足" : "不能重复下单");
        }

        // 2. 封装订单对象并转为 JSON 字符串（使用黑马点评自带的 Hutool JSONUtil）
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        String orderJson = JSONUtil.toJsonStr(voucherOrder);

        // 3. 【关键修复3】发送消息到 Kafka（以 userId 为 Key 保证同一用户的消息进同一分区）
        kafkaTemplate.send(TOPIC_SECKILL_ORDERS, userId.toString(), orderJson)
                .addCallback(
                        sendResult -> log.info("秒杀订单发送 Kafka 成功，orderId={}", orderId),
                        ex -> {
                            log.error("秒杀订单发送 Kafka 失败，回滚 Redis 预扣库存，orderId={}", orderId, ex);
                            // 发送失败时补偿回滚 Redis 库存和购买集合
                            stringRedisTemplate.opsForValue().increment("seckill:stock:" + voucherId);
                            stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
                        }
                );

        // 4. 立即返回订单 ID
        return Result.ok(orderId);
    }

    /**
     * 2. Kafka 消费者监听（如果后续你想按目录树拆到 utils/listener/ 下，把下面两个方法剪切过去即可）
     */
    @KafkaListener(topics = TOPIC_SECKILL_ORDERS, groupId = "seckill-order-group")
    public void handleKafkaOrder(String message, Acknowledgment ack) {
        try {
            log.info("收到 Kafka 秒杀订单消息: {}", message);
            VoucherOrder voucherOrder = JSONUtil.toBean(message, VoucherOrder.class);

            // 加分布式锁并执行数据库落库
            handleVoucherOrder(voucherOrder);

            // 业务执行成功，手动提交 Offset
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理 Kafka 秒杀订单消息失败：{}", message, e);
            // 【关键修复4】把异常抛出去，触发 Spring Kafka 的异常重试机制，不能静默吞掉！
            throw new RuntimeException("消费秒杀订单异常，触发重试", e);
        }
    }
    /**
     * 3. Redisson 分布式锁兜底保护
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = redisLock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单, userId={}", userId);
            return;
        }
        try {
            // 通过注入的代理对象调用事务方法，确保 @Transactional 生效
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            redisLock.unlock();
        }
    }

    /**
     * 4. 真实写 MySQL 数据库（带 Spring 事务与幂等性校验）
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();

        // 1. 幂等性校验（防止 Kafka 网络抖动重复投递，或一人多单）
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            log.warn("该用户已购买过此优惠券，忽略本次重复消费！userId={}, orderId={}", userId, voucherOrder.getId());
            return;
        }

        // 2. 乐观锁扣减 MySQL 库存（stock > 0）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("MySQL 库存不足！orderId={}", voucherOrder.getId());
            return;
        }

        // 3. 保存订单到 tb_voucher_order 表
        save(voucherOrder);
        log.info("秒杀订单成功落库 MySQL！orderId={}", voucherOrder.getId());
    }

    @Override
    @Transactional
    public Result payOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        // 1. 查询订单是否存在且属于当前用户
        VoucherOrder order = getById(orderId);
        if (order == null || !order.getUserId().equals(userId)) {
            return Result.fail("订单不存在！");
        }
        if (order.getStatus() != 1) {
            return Result.fail("订单状态异常或已超时取消！");
        }

        // 2. 核心：利用 status = 1 作为乐观锁更新订单状态为已支付（status = 2）
        boolean success = update()
                .set("status", 2)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", 1) // 乐观锁条件：只有未支付状态才能改成已支付
                .update();

        if (!success) {
            // 如果更新失败，说明并发瞬间已经被定时任务关单了
            return Result.fail("订单已超时关闭，请重新下单！");
        }

        return Result.ok("支付成功！");
    }
}