package com.hari.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsTwitterFilter {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams-demo");

        //Creating topoplogy
        StreamsBuilder streamsBuilder =new StreamsBuilder();

        //Adding streams to topology and processing (construction of topology path)
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_msg");
        KStream<String,String> filteredStream = inputTopic.filter((k,v)->extractFollowersFromTweet(v)>=10000);
        filteredStream.to("filtered_tweets");

        //Building the topology
        KafkaStreams kafkaStreams =new KafkaStreams(streamsBuilder.build(),properties);

        //Starting stream
        kafkaStreams.start();

    }

    public static Integer extractFollowersFromTweet(String tweet){
        try{
            return JsonParser.parseString(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }
}
