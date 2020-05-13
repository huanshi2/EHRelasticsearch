package es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @program: EHRelasticsearch
 * @description: 向elasticsearch中添加数据
 * @author: huanshi2
 * @create: 2020-05-03 16:51
 * @email: 1557679224@qq.com
 */

public class AdataEs {
    /*
    * 功能描述: 连接elasticsearch
    * @Param:
    * @Return:
    * @Author: huanshi2
    * @Date: 2020/5/3 20:14
    */
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

    @After
    public void closeClient() {
        client.close();
    }

    @Test
    public void createIndexWithSettings() {
        /*
         * 功能描述: 测试向es中添加数据
         * @Param: []
         * @Return: void
         * @Author: huanshi2
         * @Date: 2020/5/3 20:13
         */

        AdminClient admin = client.admin();
        // 使用Admin API对索引进行操作
        IndicesAdminClient indices = admin.indices();
        // 准备创建索引
        indices.prepareCreate("player")
                // 配置索引参数
                .setSettings(
                        // 参数配置器
                        Settings.builder()// 指定索引分区的数量。shards分区，
                                .put("index.number_of_shards", 1)
                                // 指定索引副本的数量(注意：不包括本身,如果设置数据存储副本为1,实际上数据存储了2份)
                                // 由于本机只用了单节点，这里replicas设置为0，不保存副本
                                .put("index.number_of_replicas", 0))
                // 真正执行
                .get();
    }

    @Test
    public void elasticsearchSettingsPlayerMappings() throws IOException {
        /*
         * 功能描述: 向索引中添加Mapping和field
         * @Param:
         * @Return:
         * @Author: huanshi2
         * @Date: 2020/5/3 20:13
         */

        HashMap<String, Object> settings_map = new HashMap<>(2);
        // shards分区的数量1
        settings_map.put("number_of_shards", 1);
        // 副本的数量0
        settings_map.put("number_of_replicas", 0);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("dynamic", "true")
                .startObject("properties")
                // 在文档中存储、
                .startObject("id").field("type", "integer").field("store", "true").endObject()
                // 不分词,不建索引、
                .startObject("name").field("type", "text").field("index", "false").endObject()
                //
                .startObject("age").field("type", "integer").endObject()
                //
                .startObject("salary").field("type", "integer").endObject()
                // 不分词,不建建索引、
                .startObject("team").field("type", "text").field("index", "false").endObject()
                // 不分词,但是建索引、
                .startObject("position").field("type", "text").field("index", "true").endObject()
                // 即分词,又建立索引、
                .startObject("description").field("type", "text").field("store", "false").field("index", "true")
                .field("analyzer", "ik_smart").endObject()
                // 即分词,又建立索引、在文档中存储、
                .startObject("addr").field("type", "text").field("store", "true").field("index", "true")
                .field("analyzer", "ik_smart").endObject()
                .endObject()
                .endObject();

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("player");
        prepareCreate.setSettings(settings_map).addMapping("basketball", builder).get();
    }

    @Test
    public void elasticsearchCreate() throws IOException {
        /*
        * 功能描述: 创建一个索引并添加数据
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/3 20:15
        */

        HashMap<String, Object> settings_map = new HashMap<>(2);
        // shards分区的数量1
        settings_map.put("number_of_shards", 1);
        // 副本的数量0
        settings_map.put("number_of_replicas", 0);

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("teacher");

        prepareCreate.setSettings(settings_map).get();

        IndexResponse response = client.prepareIndex("teacher", "classtwo", "11")
                .setSource(jsonBuilder().startObject().field("name", "华雄").field("sex", "man")
                        .field("deadday", new Date()).field("age", 30).field("message", "using java to control Elasticsearch")
                        .endObject())
                .get();
        System.out.println(response.toString());
    }

    @Test
    public void elasticsearchGet() throws IOException {
        /*
         * 功能描述: 查找单条信息
         * @Param:
         * @Return:
         * @Author: huanshi2
         * @Date: 2020/5/3 20:16
         */
        GetResponse response = client.prepareGet("student", "classone", "1").get();
        System.out.println(response.getSourceAsString());
    }

    @Test
    public void elasticsearchInsert() throws IOException {
        /*
        * 功能描述: 插入多条数据
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/3 20:16
        */

        IndexResponse response1 = client.prepareIndex("student", "classone", "6")
                .setSource(jsonBuilder().startObject().field("name", "王八").field("sex", "man")
                        .field("birthday", new Date()).field("age", 26).field("message", "using java to control Elasticsearch")
                        .endObject())
                .get();
        IndexResponse response2 = client.prepareIndex("student", "classone", "5")
                .setSource(jsonBuilder().startObject().field("name", "zhaoqi").field("sex", "man")
                        .field("birthday", new Date()).field("age", 25).field("message", "using java to control Elasticsearch")
                        .endObject())
                .get();

        System.out.println(response1.toString());
        System.out.println(response1.toString());
    }

    @Test
    public void elasticsearchMultiGet() throws IOException {
        /*
        * 功能描述: 插入多条数据并遍历打印
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/3 20:17
        */
        IndexResponse response1 = client.prepareIndex("student", "classone", "2")
                .setSource(jsonBuilder().startObject().field("name", "lisi").field("sex", "man")
                        .field("birthday", new Date()).field("age", 22).field("message", "using java to control Elasticsearch")
                        .endObject())
                .get();
        IndexResponse response2 = client.prepareIndex("student", "classone", "3")
                .setSource(jsonBuilder().startObject().field("name", "wangwu").field("sex", "man")
                        .field("birthday", new Date()).field("age", 23).field("message", "using java to control Elasticsearch")
                        .endObject())
                .get();

        //查找多个
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("student", "classone", "1","2","3").get();

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
    public void elasticsearchAddPlayer() throws IOException {
        /*
        * @description: 向指定索引指定Type的id的信息添加信息
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/4 14:43
        */
        IndexResponse response = client.prepareIndex("student", "classone", "10")
                .setSource(jsonBuilder().startObject()
                        .field("name", "关羽")
                        .field("birthday", new Date())
                        .field("age", 27)
                        .field("message", "win9斩华雄")
                        .endObject())
                .get();
        System.out.println(response);
    }

}