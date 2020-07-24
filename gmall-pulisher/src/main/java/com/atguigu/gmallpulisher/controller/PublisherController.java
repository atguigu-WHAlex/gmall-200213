package com.atguigu.gmallpulisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpulisher.bean.Option;
import com.atguigu.gmallpulisher.bean.SaleInfo;
import com.atguigu.gmallpulisher.bean.Stat;
import com.atguigu.gmallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
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
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.查询Phoenix获取数据
        Integer dauTotal = publisherService.getDauTotal(date);
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

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

        //5.创建Map用于存放GMV数据
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmountTotal);

        //6.将3个Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //7.返回最终结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {

        //创建Map用于存放结果数据
        Map result = new HashMap<String, Map>();

        //获取昨天日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //创建今天以及昨天分时数据的Map
        Map todayMap = null;
        Map yesterdayMap = null;

        //请求日活分时统计数据
        if ("dau".equals(id)) {
            //1.获取今天的分时统计数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //2.获取昨天的分时统计数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        } else if ("order_amount".equals(id)) {
            //1.获取今天的分时统计数据
            todayMap = publisherService.getOrderAmountHourMap(date);
            //2.获取昨天的分时统计数据
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        } else if ("new_mid".equals(id)) {
            todayMap = new HashMap();
            todayMap.put("05", 50);
            todayMap.put("08", 150);
            todayMap.put("13", 800);

            yesterdayMap = new HashMap();
            yesterdayMap.put("04", 56);
            yesterdayMap.put("07", 15);
            yesterdayMap.put("15", 230);
        }

        //3.将2天的分时数据放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //返回结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) {

        //定义Map用于存放最终返回值
        HashMap<String, Object> result = new HashMap<>();

        //查询ES中的数据
        Map saleMap = publisherService.getSaleDetail(date, startpage, size, keyword);

        Long total = (Long) saleMap.get("total");

        List<Map> saleDetailList = (List) saleMap.get("detail");
        Map ageMap = (Map) saleMap.get("ageMap");
        Map genderMap = (Map) saleMap.get("genderMap");

        //  genderMap 整理成为  OptionGroup
        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");

        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;

        List<Option> genderOptions = new ArrayList<>();
        genderOptions.add(new Option("男", maleRate));
        genderOptions.add(new Option("女", femaleRate));

        Stat genderOptionGroup = new Stat("性别占比", genderOptions);

        //  ageMap 整理成为  OptionGroup
        Long age_20Count = 0L;
        Long age20_30Count = 0L;
        Long age30_Count = 0L;

        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey = (String) entry.getKey();
            int age = Integer.parseInt(agekey);
            Long ageCount = (Long) entry.getValue();

            if (age < 20) {
                age_20Count += ageCount;
            } else if (age < 30) {
                age20_30Count += ageCount;
            } else {
                age30_Count += ageCount;
            }
        }

        Double age_20rate = 0D;
        Double age20_30rate = 0D;
        Double age30_rate = 0D;

        age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
        age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
        age30_rate = Math.round(age30_Count * 1000D / total) / 10D;

        List<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下", age_20rate));
        ageOptions.add(new Option("20岁到30岁", age20_30rate));
        ageOptions.add(new Option("30岁以上", age30_rate));
        Stat ageOptionGroup = new Stat("年龄占比", ageOptions);

        List<Stat> optionGroupList = new ArrayList<>();
        optionGroupList.add(ageOptionGroup);
        optionGroupList.add(genderOptionGroup);

        result.put("total", total);
        result.put("stat", optionGroupList);
        result.put("detail", saleDetailList);

        return JSON.toJSONString(result);
    }

}
