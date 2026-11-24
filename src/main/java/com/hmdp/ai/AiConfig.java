package com.hmdp.ai;

import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.MemoryId;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AiConfig {

    public interface SmartCustomerService {
        @SystemMessage("你是本地生活服务平台的智能客服助手。你可以帮用户查询店铺信息，也可以帮用户预约到店。当用户要预约但缺少时间或电话时，请礼貌向用户确认。")
        String chat(@MemoryId String sessionId, @UserMessage String userMessage);
    }

    @Bean
    public SmartCustomerService smartCustomerService(
            ChatLanguageModel chatLanguageModel,
            RedisChatMemoryStore redisChatMemoryStore,
            ShopAgentTools shopAgentTools
    ) {
        return AiServices.builder(SmartCustomerService.class)
                .chatLanguageModel(chatLanguageModel)
                .chatMemoryProvider(memoryId -> MessageWindowChatMemory.builder()
                        .id(memoryId)
                        .maxMessages(10)
                        .chatMemoryStore(redisChatMemoryStore)
                        .build())
                .tools(shopAgentTools)
                .build();
    }
}