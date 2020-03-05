package com.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer extends Thread{

    public static void main(String[] args) {
        //https://app.bonsai.io/clusters/yateshtwitter-tweets-2888484084/console
        new ElasticSearchConsumer().start();
    }

    public void run(){


        RestHighLevelClient elasticClient = client();
        KafkaConsumer<String,String> consumer = kafkaConsumer();
        IndexRequest indexRequest = new IndexRequest("twitter","tweet");
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                if(consumer != null){
                    consumer.close();
                    System.out.println("Consumer shut down...");
                }
            }
        });
        System.out.println("start reading msgs....");



        System.out.println("msg send to elastic search");
        //while(true) {
            String jsonString = "{\"name\":\"yatesh\"}";
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            System.out.println("Msg count : "+records.count());
            records.forEach(record->{
                System.out.println("Topic : "+record.topic()+", value : "+record.value());
                indexRequest.source(record.value(), XContentType.JSON);
                //indexRequest.source(jsonString, XContentType.JSON);

            });
        try {
            IndexResponse response = elasticClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("Response : "+response.getId());
        }catch (Exception e){
            e.printStackTrace();
        }
        //}
        consumer.close();
    }
    public KafkaConsumer<String,String> kafkaConsumer(){

        String topic = "twitterTopic";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"ElasticGroupConsumer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//latest/earliest/none

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public RestHighLevelClient client(){
        //https://3uku5ajdv7:orwwjbi21v@yateshtwitter-tweets-2888484084.us-east-1.bonsaisearch.net:443
        String host = "yateshtwitter-tweets-2888484084.us-east-1.bonsaisearch.net";
        int port = 443;
        String userName = "3uku5ajdv7";
        String password = "orwwjbi21v";


        final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https")).
                setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
