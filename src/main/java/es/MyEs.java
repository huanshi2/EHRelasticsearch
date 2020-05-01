package es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @program: EHRelasticsearch
 * @description: 测试类
 * @author: huanshi2
 * @create: 2020-05-01 18:13
 * @email: 1557679224@qq.com
 */
public class MyEs {

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
        HashMap<String, Object> settings_map = new HashMap<String, Object>(2);
        // shards分区的数量1
        settings_map.put("number_of_shards", 1);
        // 副本的数量0
        settings_map.put("number_of_replicas", 0);

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("people");
        // 管理索引（user_info）然后关联type（user）
        prepareCreate.setSettings(settings_map).get();;

        IndexResponse response = client.prepareIndex("people", "student", "3")
                .setSource(jsonBuilder().startObject().field("username", "王五五").field("sex", "男")
                        .field("birthday", new Date()).field("age", 21).field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        System.out.println(response.toString());
    }

}