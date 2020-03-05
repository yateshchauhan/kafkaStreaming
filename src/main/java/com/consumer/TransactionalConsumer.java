package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TransactionalConsumer {
    private static String topic = "newTransactionTopic";
    private static String partitionNo;
    private static List<TopicPartition> partitions = new ArrayList<TopicPartition>(){
        {
            add(new TopicPartition(topic,0));
            add(new TopicPartition(topic,1));
            add(new TopicPartition(topic,2));
        }
    };

    public static void main(String[] args) {

        try {

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run(){
                    System.exit(-1);
                    System.out.println("Consumer closed, Topic : "+topic+", Partition : "+partitions.get(Integer.parseInt(partitionNo)));
                }
            });

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG,"firstGroup");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter partition number to read by new consumer instance(0/1/2)");

            partitionNo = reader.readLine();

            KafkaConsumer consumer = new KafkaConsumer(props);

            consumer.assign(Arrays.asList(partitions.get(Integer.parseInt(partitionNo))));
            consumer.seek(partitions.get(Integer.parseInt(partitionNo)), 0);

            while(true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(2000));

                records.forEach(record -> {
                    System.out.println("Topic : " + record.topic() + ", Partition : " + record.partition() + ", key :" + record.key()
                            + ", Value : " + record.value());
                });
            }


        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
