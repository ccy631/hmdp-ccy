package com.hmdp.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String SECKILL_ORDER_TOPIC = "hmdp.seckill.orders";
    public static final String CACHE_REBUILD_TOPIC = "hmdp.cache.rebuild";

    @Bean
    public NewTopic seckillOrderTopic() {
        return TopicBuilder.name(SECKILL_ORDER_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cacheRebuildTopic() {
        return TopicBuilder.name(CACHE_REBUILD_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}