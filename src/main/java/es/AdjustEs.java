package es;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @program: EHRelasticsearch
 * @description: 修改，更新elasticsearch中的数据
 * @author: huanshi2
 * @create: 2020-05-03 19:56
 * @email: 1557679224@qq.com
 */
public class AdjustEs {
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
        * @Date: 2020/5/3 20:17
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
    public void elasticsearchUpdate() throws Exception {
        /*
        * @description: 更新指定index指定type指定id的信息
        * @Param: []
        * @Return: void
        * @Author: huanshi2
        * @Date: 2020/5/4 14:54
        */
        // 创建一个更新的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        // 指定索引Index
        updateRequest.index("student");
        // 指定类型Type
        updateRequest.type("classone");
        // 指定id的值
        updateRequest.id("9");
        // 设置修改的字段信息
        updateRequest.doc(jsonBuilder().startObject().field("name", "吕布").field("sex","man").endObject());
        // 开始进行修改，并且返回响应信息
        UpdateResponse updateResponse = client.update(updateRequest).get();
        // 打印输出响应的信息
        System.out.println(updateResponse.toString());
    }

}