package com.javastudio.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerWithCallback {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallback.class);
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
        producer.send(record, (metadata, e) -> {
            if (e == null) {
                logger.info("Message published to the kafka topic with the specifications:\n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp()
                );
            } else {
                logger.error("Error while publishing the message", e);
            }
        });
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
