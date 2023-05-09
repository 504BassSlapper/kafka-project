package org.sec.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.sec.constant.ConfigConst;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StreamConsumer {


    public static void main(String[] args) {

        new StreamConsumer();
    }

    public StreamConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConst.KAFKA_BROKER_URL.getValue());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumers-group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(ConfigConst.TOPICNAME.getValue()));

        Executors.newScheduledThreadPool(2)
                .scheduleAtFixedRate(() -> {
                    System.out.println("-----------------------consumer");
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                    consumerRecords.forEach(cr -> {
                        System.out.println("consumer record  key : " + cr.key()
                                + " of value : " + cr.value() + " in topic : " + cr.topic()
                        + " offset : " +cr.offset());
                    });

                }, 1000, 2000, TimeUnit.MILLISECONDS);


    }
}
