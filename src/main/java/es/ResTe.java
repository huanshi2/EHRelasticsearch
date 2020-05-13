package es;

import com.oracle.webservices.internal.api.databinding.DatabindingMode;
import com.sun.deploy.security.ruleset.DRSResult;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;
import lombok.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @program: EHRelasticsearch
 * @author: huanshi2
 * @create: 2020-05-13 13:48
 * @email: 1557679224@qq.com
 * @description: highlevelrestclient test
 */
public class ResTe {

    public  static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("properties")
                    .startObject()
                    .field("id").startObject().field("type", "text").endObject()
                    .field("name").startObject().field("type", "text").endObject()
                    .field("age").startObject().field("type", "integer").endObject()
                    .field("sex").startObject().field("type", "text").endObject()
                    .field("message").startObject().field("type", "text").field("analyzer", "ik_max_word").endObject()
                    .endObject()
                .endObject();

        System.out.println( builder.toString() );

        CreateIndexRequest request = new CreateIndexRequest("school");
        request.settings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0));
        request.mapping(builder);
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
        client.close();

    }

}