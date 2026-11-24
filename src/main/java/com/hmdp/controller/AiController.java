package com.hmdp.controller;

import com.hmdp.ai.AiConfig.SmartCustomerService;
import com.hmdp.dto.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/ai")
public class AiController {

    @Resource
    private SmartCustomerService smartCustomerService;

    @GetMapping("/chat")
    public Result chat(@RequestParam("sessionId") String sessionId,
                       @RequestParam("message") String message) {
        String reply = smartCustomerService.chat(sessionId, message);
        return Result.ok(reply);
    }
}