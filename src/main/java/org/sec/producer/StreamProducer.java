package org.sec.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.sec.constant.ConfigConst;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StreamProducer {
    private static int counter;

    public static void main(String[] args) {
        new StreamProducer();

    }

    public StreamProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConst.KAFKA_BROKER_URL.getValue());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "java.client.id.1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Executors.newScheduledThreadPool(2).scheduleAtFixedRate(() -> {
            System.out.println("-------------------------------------");
            ++counter;
            String message = String.valueOf(Math.random() * 1000);
            producer.send(new ProducerRecord<>(ConfigConst.TOPICNAME.getValue(), String.valueOf(++counter), message),
                    (metadata, ex) -> {
                        System.out.println("Sending message : " + counter + " value : " + message);
                        System.out.println("Partition : " + metadata.partition() + " offset : " + metadata.offset());

                    });
        }, 3000, 50, TimeUnit.MILLISECONDS);
    }
}
