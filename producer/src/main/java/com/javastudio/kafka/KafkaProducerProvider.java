package com.javastudio.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerProvider {
    private Properties properties = new Properties();
    private KafkaProducer<String, String> producer;

    void loadProperties(String resourceName) throws IOException {
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(resourceName)) {
            properties.load(in);
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public void init(String resourceName) {
        try {
            loadProperties(resourceName);
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publish(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
    }

    public void close(){
        producer.flush();
        producer.close();
    }
}
