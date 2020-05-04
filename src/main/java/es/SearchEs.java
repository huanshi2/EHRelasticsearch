package es;


import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

/**
 * @program: EHRelasticsearch
 * @author: huanshi2
 * @create: 2020-05-04 15:03
 * @email: 1557679224@qq.com
 * @description: 查询数据
 */
public class SearchEs {
    private TransportClient client = null;
    // 在所有的测试方法之前执行连接elasticsearch操作
    @SuppressWarnings("resource")
    @Before
    public void init() throws Exception {
        // 设置集群名称 elasticsearch-cluster
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch-cluster")
                // 自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .put("client.transport.sniff", true).build();
        // 创建client
        client = new PreBuiltTransportClient(settings).addTransportAddresses(
                // 建议指定2个及其以上的节点。
                //节点个数由你安装的节点决定
                new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    /*
    * @description: 测试完成后关闭连接
    * @Param: []
    * @Return: void
    * @Author: huanshi2
    * @Date: 2020/5/4 15:05
    */
    @After
    public void closeClient() {
        client.close();
    }

    @Test
    public void elasticsearchGet() throws IOException {
        GetResponse response = client.prepareGet("class", "classone", "9").get();
        System.out.println(response.getSourceAsString());
    }

    @Test
    public void elasticsearchMultiGet() throws IOException {
        /*
        * @description: 查询出多个索引Index多个类型Type的多个id的所有信息
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/4 15:07
        */

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("student", "classone", "10")
                .add("student", "classone", "2", "3","9").add("teacher", "classtwo", "11").get();
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

    @Test
    public void elasticsearchRange() {
        /*
        * @description: 范围查找
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/4 15:29
        */
        // includeLower(true).includeUpper(false)含义是包含前面,不包含后面的
        // [21, 24)
        QueryBuilder qb = rangeQuery("age").from(21).to(24).includeLower(true).includeUpper(false);
        // 将查询条件传递进去,并将查询结果进行返回。
        SearchResponse response = client.prepareSearch("student").setQuery(qb).get();
        System.out.println(response);
    }

    /*
    * @description: 还没搞太懂怎么整后面的
    * @Param:
    * @Return:
    * @Author: huanshi2
    * @Date: 2020/5/4 15:35
    */

//    /**
//     *
//     * select
//     *
//     * team, position, count(*) as pos_count
//     *
//     * from
//     *
//     * player
//     *
//     * group by
//     *
//     * team,position;
//     *
//     *
//     */
//    @Test
//    public void elasticsearchAgg2() {
//        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
//        // 指定别名和分组的字段
//        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
//        TermsAggregationBuilder posAgg = AggregationBuilders.terms("pos_count").field("position");
//        // 添加两个聚合构建器。先按照team分组,再按照position分组。
//        builder.addAggregation(teamAgg.subAggregation(posAgg));
//        // 执行查询
//        SearchResponse response = builder.execute().actionGet();
//        // 将查询结果放入map中
//        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        // 根据属性名到map中查找
//        StringTerms teams = (StringTerms) aggMap.get("team_name");
//        // 循环查找结果
//        for (Terms.Bucket teamBucket : teams.getBuckets()) {
//            // 先按球队进行分组
//            String team = (String) teamBucket.getKey();
//            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
//            StringTerms positions = (StringTerms) subAggMap.get("pos_count");
//            // 因为一个球队有很多位置，那么还要依次拿出位置信息
//            for (Terms.Bucket posBucket : positions.getBuckets()) {
//                // 拿到位置的名字
//                String pos = (String) posBucket.getKey();
//                // 拿出该位置的数量
//                long docCount = posBucket.getDocCount();
//                // 打印球队，位置，人数
//                System.out.println(team + ": " + pos + ": " + docCount);
//            }
//        }
//
//    }
//
//    /**
//     * select team, max(age) as max_age from player group by team;
//     */
//    @Test
//    public void elasticsearchAgg3() {
//        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
//        // 指定安球队进行分组
//        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
//        // 指定分组求最大值
//        MaxAggregationBuilder maxAgg = AggregationBuilders.max("max_age").field("age");
//        // 分组后求最大值
//        builder.addAggregation(teamAgg.subAggregation(maxAgg));
//        // 查询
//        SearchResponse response = builder.execute().actionGet();
//        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        // 根据team属性，获取map中的内容
//        StringTerms teams = (StringTerms) aggMap.get("team_name");
//        for (Terms.Bucket teamBucket : teams.getBuckets()) {
//            // 分组的属性名
//            String team = (String) teamBucket.getKey();
//            // 在将聚合后取最大值的内容取出来放到map中
//            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
//            // 取分组后的最大值
//            InternalMax ages = (InternalMax) subAggMap.get("max_age");
//            // 获取到年龄的值
//            double max = ages.getValue();
//            // 打印输出值
//            System.out.println(team + ": " + max);
//        }
//    }
//
//
//    /**
//     * select team, avg(age) as avg_age, sum(salary) as total_salary from player
//     * group by team;
//     */
//    @Test
//    public void elasticsearchAgg4() {
//        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("basketball");
//        // 指定分组字段
//        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team");
//        // 指定聚合函数是求平均数据
//        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
//        // 指定另外一个聚合函数是求和
//        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
//        // 分组的聚合器关联了两个聚合函数
//        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));
//        // 查询
//        SearchResponse response = builder.execute().actionGet();
//        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        // 按分组的名字取出数据
//        StringTerms teams = (StringTerms) aggMap.get("team_name");
//        for (Terms.Bucket teamBucket : teams.getBuckets()) {
//            // 获取球队名字
//            String team = (String) teamBucket.getKey();
//            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
//            // 根据别名取出平均年龄
//            InternalAvg avgAge = (InternalAvg) subAggMap.get("avg_age");
//            // 根据别名取出薪水总和
//            InternalSum totalSalary = (InternalSum) subAggMap.get("total_salary");
//            double avgAgeValue = avgAge.getValue();
//            double totalSalaryValue = totalSalary.getValue();
//            System.out.println(team + ": " + avgAgeValue + ": " + totalSalaryValue);
//        }
//    }

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