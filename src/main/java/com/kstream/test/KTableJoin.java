package com.kstream.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import sun.jvm.hotspot.runtime.Bytes;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

public class KTableJoin {


    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("sourceTopic");
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();
        //wordCounts.toStream().to("targetTopic", Produced.with(Serdes.String(), Serdes.Long()));
        textLines.to("targetTopic");
        KTable<String,Long> keyCount1 = textLines.filter((key,value)-> Integer.parseInt(key) > 5).groupBy((key,value)->value).count();
        KTable<String,Long> keyCount2 = textLines.filter((key,value)->Integer.parseInt(key) < 5).groupBy((key,value)->value).count();

        KTable<String,Long> joinTargetTable = keyCount1.leftJoin(keyCount2, (key,value)-> key);
        System.out.println(joinTargetTable);
        joinTargetTable.toStream().to("joinTargetTopic",Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.start();
            System.out.println("Stream state :"+streams.state().name());
        }catch (Exception e){
            streams.close();
            e.printStackTrace();
        }finally {
            try{Thread.sleep(5000);}catch (Exception e){e.printStackTrace();}
            streams.close();
        }
    }

    public BufferedReader reader(){
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader;
    }
}
