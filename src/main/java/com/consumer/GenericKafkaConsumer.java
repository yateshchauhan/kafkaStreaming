package com.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GenericKafkaConsumer {

    private static Logger log = LoggerFactory.getLogger(GenericKafkaConsumer.class);
    public static void main(String[] args)throws IOException {


        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter topic name to read data.");
        String topic = reader.readLine();
        System.out.println("Received topic name : "+topic);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "firstGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                if(consumer != null)
                    consumer.close();
            }
        });
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            if (consumerRecords != null) {
                consumerRecords.forEach(record -> {
                    System.out.println("Topic :"+record.topic()+", key :"+record.key()+" , value :"+record.value());
                    //System.out.println(("Topic :" + record.topic() + ", Partitions : " + record.partition() + ", Offset : " + record.offset() + ", Key :" + record.key()
                      //      + ",Values :" + record.value()));
                });
            }
        }
    }
}
