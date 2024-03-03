package com.rbhatiya.cabbookuser.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;


@Service
public class LocationService {


    @KafkaListener(topics = "cab-location", groupId = "user-group")
    public void consume(String message) {
        System.out.println(message);
    }

    //    @KafkaListener(topics = "cab-location", groupId = "user-group")
//    public void consume(String message, String key) {
//
//    }
    @KafkaListener(topics = "cab-location-topic", groupId = "user-group")
    public void consumeMessage() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers","localhost:9092,localhost:29092,localhost:39092");
        consumerProps.put("spring.kafka.consumer.group-id","user-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "user-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("cab-location-topic"));


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    // Commit the offset
                    Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                    commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitSync(commitData);
                }
            }
        } finally {
            consumer.close();
        }

    }
}
