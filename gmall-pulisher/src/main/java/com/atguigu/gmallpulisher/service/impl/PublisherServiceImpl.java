package com.atguigu.gmallpulisher.service.impl;

import com.atguigu.gmallpulisher.mapper.DauMapper;
import com.atguigu.gmallpulisher.mapper.GmvMapper;
import com.atguigu.gmallpulisher.service.PublisherService;
import io.searchbox.client.JestClient;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private GmvMapper gmvMapper;

    @Autowired
    private JestClient jestClient;

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

        //3.遍历list,将数据取出放入result
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回结果
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {

        //1.获取分时数据
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放转换结构之后的数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list,将数据取出放入result
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回
        return result;
    }

    @Override
    public Map getSaleDetail(String date, int startpage, int size, String keyword) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex("gmall200213_sale_detail-query").addType("_doc").build();

        Map resultMap = new HashMap();  //需要总数， 明细，2个聚合的结果
        try {
            SearchResult searchResult = jestClient.execute(search);

            //总数
            Long total = searchResult.getTotal();

            //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map> saleDetailList = new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add(hit.source);
            }

            //年龄聚合结果
            Map ageMap = new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                ageMap.put(bucket.getKey(), bucket.getCount());
            }

            //性别聚合结果
            Map genderMap = new HashMap();
            List<TermsAggregation.Entry> genderbuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            for (TermsAggregation.Entry bucket : genderbuckets) {
                genderMap.put(bucket.getKey(), bucket.getCount());
            }

            resultMap.put("total", total);
            resultMap.put("detail", saleDetailList);
            resultMap.put("ageMap", ageMap);
            resultMap.put("genderMap", genderMap);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;

    }

}
