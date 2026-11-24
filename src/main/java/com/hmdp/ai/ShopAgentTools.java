package com.hmdp.ai;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class ShopAgentTools {

    @Resource
    private IShopService shopService;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Tool("根据商家名称或关键词查询店铺信息、地址、均价和评分")
    public String queryShopByName(@P("商家名称或关键词，例如：茶百道、火锅") String keyword) {
        List<Shop> shops = shopService.lambdaQuery()
                .like(Shop::getName, keyword)
                .last("LIMIT 3")
                .list();
        if (shops.isEmpty()) {
            return "未查询到匹配“" + keyword + "”的店铺。";
        }
        return JSONUtil.toJsonStr(shops);
    }

    @Tool("为用户办理到店预约，必须包含商家名称、预约时间和联系电话")
    public String reserveShop(
            @P("商家名称") String shopName,
            @P("预约时间，例如：明天下午3点 或 2026-11-20 15:00") String reserveTime,
            @P("用户联系电话") String phone
    ) {
        jdbcTemplate.update(
                "INSERT INTO tb_shop_reservation (shop_name, reserve_time, phone) VALUES (?, ?, ?)",
                shopName, reserveTime, phone
        );
        log.info("新增到店预约：shopName={}, reserveTime={}, phone={}", shopName, reserveTime, phone);
        return "预约已记录！商家：" + shopName + "，时间：" + reserveTime + "，联系电话：" + phone;
    }
}