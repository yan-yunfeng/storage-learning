package curd;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Collections;

import connector.Connectors;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午3:01
 */
public class ElasticSearch {

    public static void main(String[] args) throws Exception {
        RestClient restClient = Connectors.getRestClient();

        String _id = "_id";

        //upsert
        String updateEndpoint = "/index/type/" + _id + "/_update";
        String upsertCcontent = "{\n" +
            "  \"doc\" : {\n" +
            "    \"key1\" : \"value1\",\n" +
            "    \"key2\" : \"value2\"\n" +
            "  },\n" +
            "  \"doc_as_upsert\" : true,\n" +
            "  \"retry_on_conflict\" : 3\n" +
            "}";
        HttpEntity upsertEntity = new NStringEntity(upsertCcontent, ContentType.APPLICATION_JSON);
        restClient.performRequest("POST", updateEndpoint, Collections.emptyMap(), upsertEntity);

        //update
        String updateContent = "{\n" +
            "  \"doc\" : {\n" +
            "    \"key1\" : \"value1\",\n" +
            "    \"key2\" : \"value2\"\n" +
            "  },\n" +
            "  \"retry_on_conflict\" : 3\n" +
            "}";
        HttpEntity updateEntity = new NStringEntity(updateContent, ContentType.APPLICATION_JSON);
        restClient.performRequest("POST", updateEndpoint, Collections.emptyMap(), updateEntity);

        //delete
        String deleteEndpoint = "/index/type/" + _id;
        Response response = restClient.performRequest("DEAD", deleteEndpoint);
        if (response.getStatusLine().getStatusCode() != 404) {
            restClient.performRequest("DELETE", deleteEndpoint);
        }

        //find
        String findEndpoint = "index/type/_search";
        String query = "{\n" +
            "  \"query\": {\n" +
            "    \"match_all\": {}\n" +
            "  }\n" +
            "}";
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        Response findResponse = restClient.performRequest("GET", findEndpoint, Collections.emptyMap(), entity);
        String resultJson = EntityUtils.toString(findResponse.getEntity());
        System.out.println(resultJson);


        //以下为高等级的客户端
        HttpHost[] httpHosts = {new HttpHost("hostname", 111)};
        RestClientBuilder builder = RestClient.builder(httpHosts);
        RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    }
}
