package com.atguigu.realtime.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;



// @Controller
// @ResponseBody
@RestController
public class LoggerController {
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log) {
        // 给日志添加时间戳
        log = addTs(log);
        // System.out.println(log);

        // 把数据落盘，给离线需求使用
        saveToDisk(log);

        // 把数据发送到kafka
        sendToKafka(log);
        return "ok";
    }

    @Autowired
    KafkaTemplate<String, String> kafka;
    private void sendToKafka(String log) {
        // 不同的日志写入到不同的topic
        if (log.contains("startup")) {
            // 向topic写数据，如果topic不存在，则自动创建，按照默认配置，partitions， replications
            kafka.send(Constant.START_LOG_TOPIC, log);
        } else {
            kafka.send(Constant.EVENT_LOG_TOPIC, log);
        }
    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    private void saveToDisk(String log) {
        logger.info(log);
    }

    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
         return obj.toJSONString();
    }

}
