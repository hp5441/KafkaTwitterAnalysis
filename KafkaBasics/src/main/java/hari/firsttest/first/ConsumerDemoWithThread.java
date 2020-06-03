package hari.firsttest.first;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "sixth_group";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating Consumer Thread");
        Runnable myConsumerRunnable = new ConsumerThread(bootstrapServer,groupId,topic,latch);

        Thread myConsumerThread = new Thread(myConsumerRunnable);

        myConsumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("ConsumerThread got interrupted",e);
        }finally {
            logger.info("Application shutdown");
        }
    }

    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger;

        public ConsumerThread(String bootstrapServer,
                              String groupId,
                              String topic,
                              CountDownLatch latch) {
            this.latch=latch;

            Properties properties = new Properties();
            logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            }
            catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}


