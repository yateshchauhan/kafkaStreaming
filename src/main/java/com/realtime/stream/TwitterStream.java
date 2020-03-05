package com.realtime.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Properties;

public class TwitterStream {

    String consumerKey = "k0Ty0wxy4niQyyUqW12olfclf";
    String consumerSecret = "xdnE9N0iQ8E1dNCibsEOXkWCzHfSZH3SgsnOvThquuy1KdHQds";
    String token = "1233632559231143937-lBYRQu1k9zbTGL4rao52Gb35w8ZHrP";
    String secret = "PEzVpdNX97SKiS3CQdoHutP2CRKMwHmvN8GphKEM6oxIL";
    String topicName = "twitterTopic";
    String filterTopic = "importantTweetsTopic";

    KafkaStreams kafkaStreams = null;

    public static void main(String[] args) {
        new TwitterStream().kafkaStream();
    }
    public void kafkaStream(){

        try {

            Serde<String> stringSerdes = Serdes.String();

            Properties props = new Properties();

            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-stream");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



            StreamsBuilder streamsBuilder = new StreamsBuilder();

            KStream<String, String> kStream = streamsBuilder.stream("twitterTopic");
            kStream.filter((key, value) -> true/*{
                System.out.println("going to push data into 'importantTweetsTopic'");
                //return parseTweets(value) >= 0;
            }*/).to("importantTweetsTopic");
            kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

            kafkaStreams.start();
            System.out.println("kafka stream started....");
        }catch(Exception e){
            if(kafkaStreams != null)
                kafkaStreams.close();
            e.printStackTrace();
        }

    }

    public int parseTweets(String jsonMessage){

        System.out.println("Tweet JSON message : "+jsonMessage);
        int tweetCount = 0;
        JSONParser parser = new JSONParser();
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(jsonMessage);
            jsonObject = (JSONObject) jsonObject.get("user");
            int count = (int)jsonObject.get("followers_count");
            System.out.println("followers : "+count);
            return count;
        }catch(ParseException e){
            e.printStackTrace();
            return 0;
        }

    }
}
