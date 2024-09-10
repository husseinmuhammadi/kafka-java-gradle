package com.javastudio.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaProducerProviderTest {

    private KafkaProducerProvider kafkaProducerProvider;

    private static final String TOPIC_NAME= "";

    @BeforeEach
    void setUp() {
        kafkaProducerProvider = new KafkaProducerProvider();
        kafkaProducerProvider.init("application.properties");
    }

    @AfterEach
    void tearDown() {
        kafkaProducerProvider.close();
    }

    @Test
    void loadProperties() throws Exception {
        kafkaProducerProvider.loadProperties("application.properties");
        assertEquals("localhost:9092", kafkaProducerProvider.getProperties().get("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", kafkaProducerProvider.getProperties().get("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", kafkaProducerProvider.getProperties().get("value.serializer"));
    }

    @Test
    void should_publish_message_to_kafka_topics(){
        kafkaProducerProvider.publish(TOPIC_NAME, );
    }
}