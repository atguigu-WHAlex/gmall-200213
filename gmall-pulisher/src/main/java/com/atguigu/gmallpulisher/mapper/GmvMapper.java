package com.atguigu.gmallpulisher.mapper;

import java.util.List;
import java.util.Map;

public interface GmvMapper {

    //查询GMV总数
    public Double selectOrderAmountTotal(String date);

    //查询GMV分时统计结果
    public List<Map> selectOrderAmountHourMap(String date);

}
