package com.producer;

import com.utility.MyPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = null;
        try {
            String topic = "firstTopic";


            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

            //props.put("batch.size", 16384);
            //props.put("linger.ms", 1);
            //props.put("buffer.memory", 33554432);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


            producer = new KafkaProducer<String, String>(props);

            for (int i = 0; i < 10; i++) {
                System.out.println("sending : "+String.valueOf("Data" + (i + 1)));
                ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic, String.valueOf(i + 1), String.valueOf("Data" + (i + 1)));
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null) {
                            System.out.println("Topic : "+recordMetadata.topic()+", Partition : "+recordMetadata.partition()
                            +", Offset : "+recordMetadata.offset());
                        }else{
                            System.out.println("Error caught while publishing data, "+e);
                        }
                    }
                });
                producer.flush();
                System.out.println("sent...");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(producer != null)
                producer.close();
        }


    }
}
