/**
 * @program: EHRelasticsearch
 * @description: 使用java操作elasicsearch
 * @author: huanshi2
 * @create: 2020-04-30 18:00
 * @email: 1557679224@qq.com
 */
import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport. TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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
            GetResponse response = client.prepareGet("emr", "patient", "1").execute().actionGet();
            // 输出结果
            System.out.println(response);
            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}