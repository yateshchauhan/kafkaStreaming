package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionTest {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());


        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        //consumer.subscribe(new ArrayList<String>(){{add("firstTopic");}});

        TopicPartition topicPartition = new TopicPartition("firstTopic",1);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,0);

        while (true) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record->{
                System.out.println("Topic:"+record.topic()+", Partition:"+record.partition()+", Offset:"+record.offset()
                +", Key:"+record.key()+", Value:"+record.value());
            });

        }

    }
}
