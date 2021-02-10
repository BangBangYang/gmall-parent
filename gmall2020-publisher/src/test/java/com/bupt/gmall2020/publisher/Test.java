package com.bupt.gmall2020.publisher;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author yangkun
 * @version 1.0
 * @date 2021/2/4 23:31
 */
public class Test {

    @Autowired
    JestClient jestclient;

    @org.junit.Test
    public void test() {
        String indexName = "gmall_dau_info-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
//        Search search = new Search.Builder().addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestclient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        searchResult.getHits(Map<String,Object>.class);
//        System.out.println(searchResult.getTotal());

    }
}
