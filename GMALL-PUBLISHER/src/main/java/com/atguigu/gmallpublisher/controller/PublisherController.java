package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.SaleInfo;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.lang.reflect.Array;
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

    // http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(String date, int startpage, int size, String keyword) throws IOException {
        Map<String, Object> saleDetailAndAgg = service.getSaleDetailAndAgg(date, keyword, startpage, size);
        // 创建最终的结果
        SaleInfo result = new SaleInfo();
        // 1、设置total属性
        Long total = (Long) saleDetailAndAgg.get("total");
        result.setTotal(total);
        // 2、设置详情
        ArrayList<Map> details = (ArrayList<Map>)saleDetailAndAgg.get("details");
        result.setDetail(details);
        // 3、设置饼图
        // 3.1 先设置性别的饼图
        Map<String, Long> genderAgg = (Map<String, Long>)saleDetailAndAgg.get("genderAgg");
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        // genderAgg这个Map有多少键值时，饼图就有几部分
        for (String key : genderAgg.keySet()) {
            Option option = new Option();
            option.setName(key.equals("F") ? "女" : "男");
            option.setValue(genderAgg.get(key));
            genderStat.addOption(option);
        }
        result.addStat(genderStat);
        // 3.2 年龄饼图
        Map<String, Long> ageAgg = (Map<String, Long>)saleDetailAndAgg.get("ageAgg");
        Stat ageStat = new Stat();
        ageStat.setTitle("用户年龄占比");
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及以上", 0L));

        for (Map.Entry<String, Long> entry : ageAgg.entrySet()) {
            Integer age = Integer.parseInt(entry.getKey());
            Long value = entry.getValue();
            if (age < 20) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(value + opt.getValue());
            } else if(age < 30) {
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(value + opt.getValue());
            } else {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(value + opt.getValue());
            }
        }
        result.addStat(ageStat);

        return JSON.toJSONString(result);
    }
    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }
}
