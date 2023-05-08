package org.sec.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.sec.constant.ConfigConst;

import java.util.Properties;

public class StreamProducer {
    public static void main(String[] args) {
        new StreamProducer();

    }

    public StreamProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConst.KAFKA_BROKER_URL.getValue());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "java.client.id.1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
}
