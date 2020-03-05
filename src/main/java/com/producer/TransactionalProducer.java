package com.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class TransactionalProducer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,  5000);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"Transaction_ID_NEW_MY");

        Producer<Integer,String> producer = new KafkaProducer<Integer, String>(props);

        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        String topic = "newTransactionTopic";

        int i = 0;
        try {
            producer.initTransactions();

            while (true) {

                System.out.println("Enter the msg:");
                String msg = input.readLine();
                if ("End".equalsIgnoreCase(msg)) {
                    //producer.abortTransaction();
                    break;
                } else {
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, null,msg));
                    producer.flush();
                    System.out.println("published msg : " + msg);
                    producer.commitTransaction();
                }

            }

        }catch(Exception e) {
            System.out.println("Failed ,"+e);

            producer.abortTransaction();
        }



    }
}
