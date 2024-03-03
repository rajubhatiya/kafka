package com.rbhatiya.cabbookdriver.service;

import com.rbhatiya.cabbookdriver.constant.AppConstant;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.util.*;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Service
public class CabLocationService {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static String processRecord(String value) {
        // Add your processing logic here
        return value.toUpperCase(); // For example, converting to uppercase
    }

    /**
     * @param location
     * @return
     */
    public boolean updateLocation(String location) {
        kafkaTemplate.send(AppConstant.CAB_LOCATION, location);
        return true;
    }

    /**
     *
     * @param location
     */
    public void KafkaProducerWithOffset(String location) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092,localhost:29092,localhost:39092");
        producerProps.put("spring.kafka.producer.key-serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("spring.kafka.producer.value-serializer", "org.apache.kafka.common.serialization.StringSerializer");


        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        // Produce the processed record to another topic
        ProducerRecord<String, String> record = new ProducerRecord<>(AppConstant.CAB_LOCATION, location);
        // Process the record
        String processedValue = processRecord(record.value());
        producer.send(record);
        System.out.println("Producer processed value : "+processedValue);
        producer.close();
    }

    public String createTopic(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:29092,localhost:39092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserialize", "org.apache.kafka.common.serialization.StringDeserializer");

        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic("cab-location-topic", 3, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);
        adminClient.close();
        return "Topic Created successfully";
    }
}
