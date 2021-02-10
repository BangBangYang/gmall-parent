package com.bupt.gmall2020.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yangkun
 * @version 1.0
 * @date 2021/1/24 18:04
 */

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String  applog(@RequestBody String logString  ){
        //1 输出日志
//        System.out.println(logString);
        log.info(logString);

        // 分流
        JSONObject jsonObject = JSON.parseObject(logString);
        if(jsonObject.getString("start")!=null&&jsonObject.getString("start").length()>0){
            //启动日志
            kafkaTemplate.send("GMALL_STARTUP",logString);
        }else{
            //事件日志
            kafkaTemplate.send("GMALL_EVENT",logString);
        }

        return logString;
    }
}
