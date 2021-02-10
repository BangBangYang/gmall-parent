package com.bupt.gmall2020.publisher.service.impl;

import com.bupt.gmall2020.publisher.service.EsService;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.commons.lang3.time.DateUtils;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangkun
 * @version 1.0
 * @date 2021/2/5 12:33
 */
@Service
public class EsServiceImpl implements EsService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_"+date+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            JsonObject jsonObject = searchResult.getJsonObject();

//            System.out.println(jsonObject.toString());

            JsonElement jsonElement = jsonObject.get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value");
//            System.out.println(jsonElement.getAsBigInteger());
//            String total = searchResult.getJsonString();

//            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
//            for(SearchResult.Hit<Map, Void> hit:hits){
//                Map<String,Object>source = hit.source;
//                System.out.println(source.toString());
//            }
            return  jsonElement.getAsLong();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }

    @Override
    public Map getDauHour(String date) {
        //构造查询语句
        String indexName = "gmall_dau_info_"+date+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            //封装返回结果
            Map<String,Long> aggMap=new HashMap<>();
            SearchResult searchResult = jestClient.execute(search);
            if(searchResult.getAggregations().getTermsAggregation("groupby_hr")!=null)
            {
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                for(TermsAggregation.Entry bucket:buckets){
                    aggMap.put(bucket.getKey(),bucket.getCount());
                }
            }


            return aggMap;

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
    private String getYestoday(String date){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = simpleDateFormat.parse(date);
            Date yestoday = DateUtils.addDays(today, -1);
            return simpleDateFormat.format(yestoday);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("ddd");
        }
    }

}

