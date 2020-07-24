package com.atguigu.gmallpulisher.service;

import java.util.Map;


public interface PublisherService {

    //获取日活总数
    public Integer getDauTotal(String date);

    //获取日活分时统计数
    public Map getDauTotalHourMap(String date);

    //获取GMV总数
    public Double getOrderAmountTotal(String date);

    //获取GMV分时统计结果
    public Map getOrderAmountHourMap(String date);

    public Map getSaleDetail(String date, int startpage, int size, String keyword);
}