package com.atguigu.gmallpulisher.bean;

import com.atguigu.gmallpulisher.bean.Stat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaleInfo {
    private Long total;
    private List<Stat> optionGroupList;
    private List<Map> saleDetailList;
}
