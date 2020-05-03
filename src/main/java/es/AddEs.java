package es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
//import org.elasticsearch.action.bulk.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * @program: EHRelasticsearch
 * @description: 使用java api操作Elasticsearch的增删改查以及复杂查询（聚合查询，可以进行分组统计数量，分组统计最大值，分组统计平均值，等等统计）
 * @author: huanshi2
 * @create: 2020-05-01 17:21
 * @email: 1557679224@qq.com
 */

public class AddEs {

    private TransportClient client = null;

    @SuppressWarnings("resource")
    @Before
    public void init() throws Exception {
        // 设置集群名称biehl01
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch-cluster")
                // 自动感知的功能(可以通过当前指定的节点获取所有es节点的信息)
                .put("client.transport.sniff", true).build();
        // 创建client
        client = new PreBuiltTransportClient(settings).addTransportAddresses(
                // new InetSocketTransportAddress(InetAddress.getByName("192.168.110.133"),
                // 9300),
                // new InetSocketTransportAddress(InetAddress.getByName("192.168.110.133"),
                // 9300),
                // 建议指定2个及其以上的节点。
                new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    /**
     * 创建一个Index索引、Type类型、以及id。
     *
     * 然后插入类型里面的数据。
     *
     * @throws IOException
     */
    @Test
    public void elasticsearchCreate() throws IOException {
        HashMap<String, Object> settings_map = new HashMap<>(2);
        // shards分区的数量1
        settings_map.put("number_of_shards", 1);
        // 副本的数量0
        settings_map.put("number_of_replicas", 0);

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("people");
        // 管理索引（user_info）然后关联type（user）
        prepareCreate.setSettings(settings_map).get();

        IndexResponse response = client.prepareIndex("people", "student", "3")
                .setSource(jsonBuilder().startObject().field("username", "王五五").field("sex", "男")
                        .field("birthday", new Date()).field("age", 21).field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        System.out.println(response.toString());
    }

    /**
     * 查找一条索引Index里面的类型Type里面的id的所有信息
     *
     * @throws IOException
     */
    @Test
    public void elasticsearchGet() throws IOException {
        GetResponse response = client.prepareGet("people", "student", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 查找多条
     *
     * 索引Index里面的类型Type里面的多个id的所有信息
     *
     * @throws IOException
     */
    @Test
    public void elasticsearchMultiGet() throws IOException {
        // 查询出多个索引Index多个类型Type的多个id的所有信息
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("people", "student", "1")
                .add("people", "student", "2", "3").add("people", "teacher", "1").add("news", "fulltext", "1").get();
        // 将查询出的结果遍历输出
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            // 将每一个查询出的结果遍历输出
            GetResponse response = itemResponse.getResponse();
            // 判断如果存在就进行遍历输出
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    /**
     * 修改指定的索引Index里面的类型Type的id的信息
     *
     * @throws Exception
     */
    @Test
    public void elasticsearchUpdate() throws Exception {
        // 创建一个更新的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        // 指定索引Index
        updateRequest.index("people");
        // 指定类型Type
        updateRequest.type("student");
        // 指定id的值
        updateRequest.id("3");
        // 设置修改的字段信息
        updateRequest.doc(jsonBuilder().startObject().field("username", "王五五").endObject());
        // 开始进行修改，并且返回响应信息
        UpdateResponse updateResponse = client.update(updateRequest).get();
        // 打印输出响应的信息
        System.out.println(updateResponse.toString());
    }

    /**
     * 删除指定的索引Index里面的类型Type的id的信息
     */
    @Test
    public void elasticsearchDelete() {
        // 指定删除的id信息,并且给出响应结果
        // prepareDelete(String index, String type, String id);
        DeleteResponse response = client.prepareDelete("people", "student", "4").get();
        // 打印输出的响应信息
        System.out.println(response);
    }

    /**
     * 根据查询条件进行删除数据
     *
     *
     */
    @Test
    public void elasticsearchDeleteByQuery() {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                // 指定查询条件,matchQuery是name的值text里面包括了这个内容就进行删除。默认使用标准分词器。
                .filter(QueryBuilders.matchQuery("username", "王五五"))
                // 指定索引名称
                .source("people").get();
        // 获取到删除的个数
        long deleted = response.getDeleted();
        // 打印输出删除的个数
        System.out.println(deleted);
    }

    /**
     * 异步删除
     *
     * 监听,如果真正删除以后进行回调,打印输出删除确认的消息。
     */
    @Test
    public void elasticsearchDeleteByQueryAsync() {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("sex", "男"))
                .source("people").execute(new ActionListener<BulkByScrollResponse>() {

            // 删除以后的方法回调
            @Override
            public void onResponse(BulkByScrollResponse response) {
                // 返回删除的个数
                long deleted = response.getDeleted();
                System.out.println("数据删除完毕！");
                // 打印删除的个数
                System.out.println("数据删除的个数: " + deleted);
            }

            @Override
            public void onFailure(Exception e) {
                // 失败打印异常信息
                e.printStackTrace();
            }
        });

        // 先打印输出,正常执行完毕。再执行异步监听删除数据。
        try {
            System.out.println("异步删除操作!");
            // 休眠10秒钟,避免主线程里面结束,子线程无法进行结果输出
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * 按照范围进行查找。
     *
     */
    @Test
    public void elasticsearchRange() {
        // includeLower(true).includeUpper(false)含义是包含前面,不包含后面的
        // [21, 24)
        QueryBuilder qb = rangeQuery("age").from(21).to(24).includeLower(true).includeUpper(false);
        // 将查询条件传递进去,并将查询结果进行返回。
        SearchResponse response = client.prepareSearch("people").setQuery(qb).get();
        System.out.println(response);
    }

    /**
     *
     * 向指定索引index里面的类型Type的id的信息
     *
     * @throws IOException
     */
    @Test
    public void elasticsearchAddPlayer() throws IOException {
        //
        IndexResponse response = client.prepareIndex("player", "basketball", "4")

                .setSource(jsonBuilder().startObject()

                        .field("name", "安其拉")

                        .field("age", 28)

                        .field("salary", 99000)

                        .field("team", "啦啦队 team")

                        .field("position", "打中锋")

                        .field("description", "跪族蓝孩")

                        .endObject())
                .get();

        System.out.println(response);
    }

    /**
     *
     *
     * select team, count(*) as team_count from player group by team;
     *
     * team_counts是别名称。
     */
    @Test
    public void elasticsearchAgg1() {
        // 指定索引和type
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
        // 按team分组然后聚合，但是并没有指定聚合函数。
        // team_count是别名称
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_count").field("team");
        // 添加聚合器
        builder.addAggregation(teamAgg);
        // 触发
        SearchResponse response = builder.execute().actionGet();
        // System.out.println(response);
        // 将返回的结果放入到一个map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        // 遍历打印输出
        Set<String> keys = aggMap.keySet();
        for (String key : keys) {
            System.out.println("key: " + key);
        }

        System.out.println("");

        // //取出聚合属性
        StringTerms terms = (StringTerms) aggMap.get("team_count");

        // //依次迭代出分组聚合数据
        for (Terms.Bucket bucket : terms.getBuckets()) {
            // 分组的名字
            String team = (String) bucket.getKey();
            // count，分组后一个组有多少数据
            long count = bucket.getDocCount();
            System.out.println(team + ": " + count);
        }

        System.out.println("");

        // 使用Iterator进行遍历迭代
        Iterator<StringTerms.Bucket> teamBucketIt = terms.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            Terms.Bucket bucket = teamBucketIt.next();
            // 获取到分组后每组的组名称
            String team = (String) bucket.getKey();
            // 获取到分组后的每组数量
            long count = bucket.getDocCount();
            // 打印输出
            System.out.println(team + ": " + count);
        }
    }

    /**
     *
     * select
     *
     * team, position, count(*) as pos_count
     *
     * from
     *
     * player
     *
     * group by
     *
     * team,position;
     *
     *
     */
    @Test
    public void elasticsearchAgg2() {
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
        // 指定别名和分组的字段
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
        TermsAggregationBuilder posAgg = AggregationBuilders.terms("pos_count").field("position");
        // 添加两个聚合构建器。先按照team分组,再按照position分组。
        builder.addAggregation(teamAgg.subAggregation(posAgg));
        // 执行查询
        SearchResponse response = builder.execute().actionGet();
        // 将查询结果放入map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        // 根据属性名到map中查找
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        // 循环查找结果
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            // 先按球队进行分组
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            StringTerms positions = (StringTerms) subAggMap.get("pos_count");
            // 因为一个球队有很多位置，那么还要依次拿出位置信息
            for (Terms.Bucket posBucket : positions.getBuckets()) {
                // 拿到位置的名字
                String pos = (String) posBucket.getKey();
                // 拿出该位置的数量
                long docCount = posBucket.getDocCount();
                // 打印球队，位置，人数
                System.out.println(team + ": " + pos + ": " + docCount);
            }
        }

    }

    /**
     * select team, max(age) as max_age from player group by team;
     */
    @Test
    public void elasticsearchAgg3() {
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
        // 指定安球队进行分组
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
        // 指定分组求最大值
        MaxAggregationBuilder maxAgg = AggregationBuilders.max("max_age").field("age");
        // 分组后求最大值
        builder.addAggregation(teamAgg.subAggregation(maxAgg));
        // 查询
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        // 根据team属性，获取map中的内容
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            // 分组的属性名
            String team = (String) teamBucket.getKey();
            // 在将聚合后取最大值的内容取出来放到map中
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            // 取分组后的最大值
            InternalMax ages = (InternalMax) subAggMap.get("max_age");
            // 获取到年龄的值
            double max = ages.getValue();
            // 打印输出值
            System.out.println(team + ": " + max);
        }
    }


    /**
     * select team, avg(age) as avg_age, sum(salary) as total_salary from player
     * group by team;
     */
    @Test
    public void elasticsearchAgg4() {
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
        // 指定分组字段
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team");
        // 指定聚合函数是求平均数据
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        // 指定另外一个聚合函数是求和
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        // 分组的聚合器关联了两个聚合函数
        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));
        // 查询
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        // 按分组的名字取出数据
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            // 获取球队名字
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            // 根据别名取出平均年龄
            InternalAvg avgAge = (InternalAvg) subAggMap.get("avg_age");
            // 根据别名取出薪水总和
            InternalSum totalSalary = (InternalSum) subAggMap.get("total_salary");
            double avgAgeValue = avgAge.getValue();
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + ": " + avgAgeValue + ": " + totalSalaryValue);
        }
    }

    /**
     * select team, sum(salary) as total_salary from player group by team order by
     * total_salary desc;
     */
//    @Test
//    public void elasticsearchAgg5() {
//        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
//        // 按team进行分组，然后指定排序规则
//        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team")
//                .order(Terms.Order.aggregation("total_salary ", true));
//        // 指定一个聚合函数是求和
//        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
//        // 添加两个聚合构建器。先按照team分组,再按照salary求和。
//        builder.addAggregation(termsAgg.subAggregation(sumAgg));
//        // 查询
//        SearchResponse response = builder.execute().actionGet();
//        // 将查询结果放入map中
//        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        // 从查询结果中获取到team_name的信息
//        StringTerms teams = (StringTerms) aggMap.get("team_name");
//        // 开始遍历获取到的信息
//        for (Terms.Bucket teamBucket : teams.getBuckets()) {
//            // 获取到key的值
//            String team = (String) teamBucket.getKey();
//            // 获取到求和的值
//            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
//            // 获取到求和的值的信息
//            InternalSum totalSalary = (InternalSum) subAggMap.get("total_salary");
//            // 获取到求和的值
//            double totalSalaryValue = totalSalary.getValue();
//            // 打印输出信息
//            System.out.println(team + " " + totalSalaryValue);
//        }
//    }

}
