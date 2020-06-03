package hari.firsttest.first;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "fourth_group";
        String topic = "first_topic";

        Properties properties = new Properties();
        Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class.getName());

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        TopicPartition topicPartition = new TopicPartition(topic,0);
        Long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,offsetToReadFrom);

        boolean keepOnReading = true;
        int lengthToRead = 5;
        int lengthCovered = 0;

        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Partition: "+record.partition()+" Offset: "+record.offset());
                lengthCovered++;
                if(lengthCovered>=lengthToRead){
                    keepOnReading=false;
                    break;
                }
            }
        }
    }
}
