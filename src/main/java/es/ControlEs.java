package es;
import es.AdminAPI;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @program: EHRelasticsearch
 * @description: 使用java操作elasicsearch
 * @author: huanshi2
 * @create: 2020-04-30 18:00
 * @email: 1557679224@qq.com
 */

public class ControlEs {
    public static void main(String[] args) {
        try {
            // 设置集群名称biehl01,Settings设置es的集群名称,使用的设计模式，链式设计模式、build设计模式。
            Settings settings = Settings.builder().put("cluster.name", "elasticsearch-cluster").build();
            // 读取es集群中的数据,创建client。
            @SuppressWarnings("resource")
            TransportClient client = new PreBuiltTransportClient(settings).addTransportAddresses(
                    new TransportAddress(InetAddress.getByName("localhost"), 9300));
            // 搜索数据(.actionGet()方法是同步的，没有返回就等待)
            // 方式是先去索引里面查询出索引数据,再去文档里面查询出数据。
            //GetResponse response = client.prepareGet("emr", "patient", "1").execute().actionGet();
            // 输出结果
            //System.out.println(response);

            AdminClient admin = client.admin();
            // 使用Admin API对索引进行操作
            IndicesAdminClient indices = admin.indices();
            // 准备创建索引
            indices.prepareCreate("people")
                    // 配置索引参数
                    .setSettings(
                            // 参数配置器
                            Settings.builder()// 指定索引分区的数量。shards分区，
                                    .put("index.number_of_shards", 1)
                                    // 指定索引副本的数量(注意：不包括本身,如果设置数据存储副本为1,实际上数据存储了2份)
                                    // 由于本机只要单节点，这里分片设置为0，不保存副本
                                    .put("index.number_of_replicas", 0))
                    // 真正执行
                    .get();

            IndexResponse response1 = client.prepareIndex("people", "student", "3")
                    .setSource(jsonBuilder().startObject().field("username", "aaa").field("sex", "man")
                            .field("birthday", new Date()).field("age", 21).field("message", "trying out Elasticsearch")
                            .endObject())
                    .get();

            System.out.println(response1.toString());

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}