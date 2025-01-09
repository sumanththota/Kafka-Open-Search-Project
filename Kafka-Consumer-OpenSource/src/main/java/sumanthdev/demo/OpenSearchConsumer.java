package sumanthdev.demo;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.*;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import static org.opensearch.client.Requests.indexRequest;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {

        //Establish credentials to use basic authentication.
        //Only for demo purposes. Don't specify your credentials in code.
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("admin", "Sumanth@98"));

        //Create a client.
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);

        //Create a non-default index with custom settings and mappings.

        return client;

    }
    public static KafkaConsumer<String, String> createKafkaConsumer(){
        String groupId = "consumer-opensearch-demo";
        String Topic = "wikimedia.recentchange";
        // create Producer Properties
        Properties properties = new Properties();
        //connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }
    public static String extractId(String json){
        JsonParser parser = new JsonParser();
        return parser.parse(json).getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }



    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        // first create the Rest Client
        RestHighLevelClient openSearchClient = OpenSearchConsumer.createOpenSearchClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();


        boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
        if(!indexExists){
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("wikimedia index has been created");
        } else {
            log.info("wikimeadia index already exists");
        }
        //get reference to the main thread

        final Thread mainThread = Thread.currentThread();

        //adding a shutdown thread
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shut down, calling consumer.wakeup()");
                consumer.wakeup();
                log.info("After calling wakeup");
                try{
                    log.info("before joinng main thread");
                    mainThread.join();
                    log.info("after joinng main thread");
                }
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

        });


        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                int recordCount = records.count();
                log.info("Recieved "+ recordCount+ " Records");
                for(ConsumerRecord<String,String> record : records){
                    //Strategy 1
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    // Strategy 2
                    String id = extractId(record.value());
                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info(response.getId());
                    } catch(Exception e){
                        log.info(String.valueOf(e));
                    }
                }

            }
            } catch(WakeupException e){
                log.info("Received shutdown signal");
            } catch (Exception e){
                log.error("Error in consumer", e);
            }

        //create our kafka client

        //main code logic

        // things


    }
}





//########################
//working
//Point to keystore with appropriate certificates for security.
//        System.setProperty("javax.net.ssl.trustStore", "/full/path/to/keystore");
//        System.setProperty("javax.net.ssl.trustStorePassword", "password-to-keystore");

//Establish credentials to use basic authentication.
//Only for demo purposes. Don't specify your credentials in code.

//Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
//final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials("admin", "Sumanth@98"));
//
////Create a client.
//RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "https"))
//        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//            }
//        });
//RestHighLevelClient client = new RestHighLevelClient(builder);
//
////Create a non-default index with custom settings and mappings.
//CreateIndexRequest createIndexRequest = new CreateIndexRequest("custom-index2");
//        log.info("CREATED index request");
//
////        createIndexRequest.settings(Settings.builder() //Specify in the settings how many shards you want in the index.
////                .put("index.number_of_shards", 4)
////                .put("index.number_of_replicas", 3)
////        );
////Create a set of maps for the index's mappings.
////        HashMap<String, String> typeMapping = new HashMap<String,String>();
////        typeMapping.put("type", "integer");
////        HashMap<String, Object> ageMapping = new HashMap<String, Object>();
////        ageMapping.put("age", typeMapping);
////        HashMap<String, Object> mapping = new HashMap<String, Object>();
////        mapping.put("properties", ageMapping);
////        createIndexRequest.mapping(mapping);
////        log.info("mapping succesful to that index request");
//CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
//        log.info("creating from create index in the client succesful");
//
////Adding data to the index.
//IndexRequest request = new IndexRequest("custom-index2"); //Add a document to the custom-index we created.
////request.id("1"); //Assign an ID to the document.
//        log.info("document added to the index succesful");
//
//HashMap<String, String> stringMapping = new HashMap<String, String>();
//        stringMapping.put("message:", "Testing Java REST client");
//        request.source(stringMapping); //Place your content into the index's source.
//IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
//        log.info("source succsfully written to the open source : index response succesful");
//
////Getting back the document
////        GetRequest getRequest = new GetRequest("custom-index2", "1");
////        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
//
////System.out.println(response.getSourceAsString());
//
////Delete the document
////        DeleteRequest deleteDocumentRequest = new DeleteRequest("custom-index", "1"); //Index name followed by the ID.
////        DeleteResponse deleteResponse = client.delete(deleteDocumentRequest, RequestOptions.DEFAULT);
//
////Delete the index
////        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("custom-index"); //Index name.
////        AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
//
//        client.close();