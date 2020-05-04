package es;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @program: EHRelasticsearch
 * @description: 删除elasticsearch中的数据
 * @author: huanshi2
 * @create: 2020-05-03 16:50
 * @email: 1557679224@qq.com
 */
public class DelectEsdata {
    private TransportClient client = null;

    // 在所有的测试方法之前执行连接elasticsearch操作
    @SuppressWarnings("resource")
    @Before
    public void init() throws Exception {
        /*
        * 功能描述: 连接elasticsearch
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/3 20:18
        */
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

    // 在所有的测试方法之后执行关闭elasticsearch
    @After
    public void closeClient() {
        client.close();
    }

    @Test
    public void elasticsearchDelete() {
        /*
        * @description: 删除指定id的数据
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/4 14:19
        */
        // 指定删除的id信息,并且给出响应结果
        DeleteResponse response = client.prepareDelete("student", "classone", "5").get();
        // 打印输出的响应信息
        System.out.println(response);
    }

    @Test
    public void elasticsearchDeleteByQuery() {
        /*
         * @description: 根据查询条件进行删除数据
         * @Param: []
         * @Return: void
         * @Author: huanshi2
         * @Date: 2020/5/4 14:27
         */
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                // 指定查询条件,matchQuery是name的值text里面包括了这个内容就进行删除。默认使用标准分词器。
                .filter(QueryBuilders.matchQuery("name", "王八"))
                // 指定索引名称
                .source("student").get();
        // 获取到删除的个数
        long deleted = response.getDeleted();
        // 打印输出删除的个数
        System.out.println(deleted);
    }
}