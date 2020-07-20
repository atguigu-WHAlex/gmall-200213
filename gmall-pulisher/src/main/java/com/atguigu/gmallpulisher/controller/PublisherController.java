package com.atguigu.gmallpulisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.查询Phoenix获取数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建Map用于存放日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建Map用于存放新增数据
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 1233);

        //5.将2个Map放入集合
        result.add(dauMap);
        result.add(newMidMap);

        //6.返回最终结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {

        //创建Map用于存放结果数据
        Map result = new HashMap<String, Map>();

        //请求日活分时统计数据
        if ("dau".equals(id)) {

            //1.获取今天的分时统计数据
            Map todayMap = publisherService.getDauTotalHourMap(date);

            //2.获取昨天的分时统计数据
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();
            Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

            //3.将2天的分时数据放入result
            result.put("yesterday", yesterdayMap);
            result.put("today", todayMap);
        }

        //返回结果
        return JSONObject.toJSONString(result);
    }

}
