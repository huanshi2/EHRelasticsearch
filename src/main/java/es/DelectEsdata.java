package es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;

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



}