package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService service;

    // http://localhost:8070/realtime-total?date=2020-08-19
    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam String date) {

        Long totalDau = service.getDau(date);
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", totalDau.toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "-1");
        result.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);

        return JSON.toJSONString(result);
    }

    // http://localhost:8070/realtime-hour?id=order_amount&date=2020-02-14
    @GetMapping("/realtime-hour")
    public String realtimeHour(String id, String date) {

        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)){
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));

            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        }
        return "ok";
    }
    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }
}
