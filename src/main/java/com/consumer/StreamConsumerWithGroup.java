package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamConsumerWithGroup {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /*
        * cfg.Consumer.Group.Session.Timeout = 20 * time.Second
		cfg.Consumer.Group.Heartbeat.Interval = 6 * time.Second
        *
        * */
        KafkaStreams streams = null;
        try{
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("sourceTopic").mapValues(value -> value).to("targetTopic");

        streams = new KafkaStreams(builder.build(), props);



        streams.start();
        System.out.println("data pipeline used to transfered data");
        }catch(Exception e){

            e.printStackTrace();
        }finally {
            try{
            Thread.sleep(10000);}catch (Exception e){e.printStackTrace();}
            streams.cleanUp();
            streams.close();
        }

    }
    public static void main1(String[] args) {

        try {
            BufferedReader reader = reader();
            System.out.println("Enter source topic");
            String sourceTopic = reader.readLine();
            System.out.println("Enter target topic");
            String targetTopic = reader.readLine();
            execute(sourceTopic, targetTopic);
            reader.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void execute(String sourceTopic, String targetTopic){

        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-kafka-stream");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> stream = builder.stream(Arrays.asList(sourceTopic));
                stream.to(targetTopic);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.out.println("data transferred from firstTopic to secondTopic.....");

    }

    public static BufferedReader reader(){
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new InputStreamReader(System.in));
        }catch (Exception e){
            e.printStackTrace();
        }
        return reader;
    }
}
