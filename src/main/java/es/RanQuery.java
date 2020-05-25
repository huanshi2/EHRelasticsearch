package es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
* @description:
* @Param:
* @Return:
* @Author: huanshi2
* @Date: 2020/5/25 15:08
*/

public class RanQuery {

    private RestHighLevelClient  client = null;

    @Before
    public void init() throws Exception {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @After
    public void closeClient() throws IOException {
        client.close();
    }

    @Test
    public void RanQuy() throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        // 设置request要搜索的索引和类型
        searchRequest.indices("school").types("_doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"_id","age","name","sex","message"},new String[]{});
        searchSourceBuilder.sort("age", SortOrder.ASC);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders
                .rangeQuery("age")
                .from(60).to(100);

        searchSourceBuilder.query(rangeQueryBuilder);

        try {
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            System.out.println("searchRequest = " + searchRequest);
            System.out.println("searchSourceBuilder = " + searchSourceBuilder);
            System.out.println("searchResponse = " + searchResponse);

            //查询响应中取出结果
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();

            for (SearchHit hit : searchHits) {
                System.out.println(hit.getSourceAsString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }



}
