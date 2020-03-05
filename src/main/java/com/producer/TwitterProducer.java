package com.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

    String consumerKey = "k0Ty0wxy4niQyyUqW12olfclf";
    String consumerSecret = "xdnE9N0iQ8E1dNCibsEOXkWCzHfSZH3SgsnOvThquuy1KdHQds";
    String token = "1233632559231143937-lBYRQu1k9zbTGL4rao52Gb35w8ZHrP";
    String secret = "PEzVpdNX97SKiS3CQdoHutP2CRKMwHmvN8GphKEM6oxIL";
    static String topicName = "twitterTopic";

    public static void main(String[] args) {
        TwitterProducer twitter = new TwitterProducer();


        Client twitterClient = twitter.twitterClient();
        KafkaProducer<String,String> kafkaProducer = twitter.getKafkaProducer();
        twitter.addShutdownHook(twitterClient,kafkaProducer);
        long msgCount = 0;
        while (!twitterClient.isDone()) {
            try {
                String msg = twitter.msgQueue.take();

                System.out.println("sending msg : "+msg);
                kafkaProducer.send(new ProducerRecord<>(topicName,String.valueOf(++msgCount),msg));
                kafkaProducer.flush();
            }catch(Exception e){
                twitterClient.stop();
                kafkaProducer.close();

            }
        }
    }

    public KafkaProducer<String,String> getKafkaProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //safe
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.RETRIES_CONFIG, "2");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        //high throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(8*1024));
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    public Client twitterClient(){

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("Kafka","politics","usa","developer","account","data","modi");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();

        return hosebirdClient;
    }

    public void addShutdownHook(Client twitterClient , KafkaProducer<String,String> kafkaProducer){
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                if(twitterClient != null)
                    twitterClient.stop();
                if(kafkaProducer != null)
                    kafkaProducer.close();
                System.out.println("Twitter/Kafka both got closed...");
            }
        });
    }

}
