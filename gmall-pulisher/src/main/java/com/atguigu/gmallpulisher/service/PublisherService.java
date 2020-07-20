package com.atguigu.gmallpulisher.service;

import java.util.Map;

public interface PublisherService {

    //获取日活总数
    public Integer getDauTotal(String date);

    //获取日活分时统计数
    public Map getDauTotalHourMap(String date);
}