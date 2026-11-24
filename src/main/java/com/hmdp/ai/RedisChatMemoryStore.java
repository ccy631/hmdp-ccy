package com.hmdp.ai;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ChatMessageDeserializer;
import dev.langchain4j.data.message.ChatMessageSerializer;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class RedisChatMemoryStore implements ChatMemoryStore {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final String KEY_PREFIX = "ai:chat:memory:";

    @Override
    public List<ChatMessage> getMessages(Object memoryId) {
        String json = stringRedisTemplate.opsForValue().get(KEY_PREFIX + memoryId);
        if (json == null || json.isEmpty()) {
            return new ArrayList<>();
        }
        return ChatMessageDeserializer.messagesFromJson(json);
    }

    @Override
    public void updateMessages(Object memoryId, List<ChatMessage> messages) {
        String json = ChatMessageSerializer.messagesToJson(messages);
        stringRedisTemplate.opsForValue().set(KEY_PREFIX + memoryId, json, 2, TimeUnit.HOURS);
    }

    @Override
    public void deleteMessages(Object memoryId) {
        stringRedisTemplate.delete(KEY_PREFIX + memoryId);
    }
}