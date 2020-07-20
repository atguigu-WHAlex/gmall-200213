package com.atguigu.gmallpulisher.service.impl;

import com.atguigu.gmallpulisher.mapper.DauMapper;
import com.atguigu.gmallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    //获取日活总数
    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //获取日活分时统计数
    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询日活分时统计数
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放转换结构之后的数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list,将数据取出防入result
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回结果
        return result;
    }

}
