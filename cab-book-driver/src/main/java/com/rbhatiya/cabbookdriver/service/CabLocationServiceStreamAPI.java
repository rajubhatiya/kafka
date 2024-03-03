package com.rbhatiya.cabbookdriver.service;

import com.rbhatiya.cabbookdriver.constant.AppConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class CabLocationServiceStreamAPI {
// props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:29092,localhost:39092");
    public String produceUskingKafkaSream() {
        // Kafka Streams configuration for the producer
        Properties producerProps = new Properties();
        producerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-producer-app");
        producerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:29092,localhost:39092");
        producerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        producerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create Kafka Streams builder for the producer
        StreamsBuilder producerBuilder = new StreamsBuilder();

        // Create a producer stream to produce 100 messages
        KStream<String, String> producerStream = producerBuilder.stream(AppConstant.CAB_LOCATION);
        for (int i = 0; i < 100; i++) {
            System.out.println("Serdes.String() "+Serdes.String());
            producerStream.to(AppConstant.CAB_LOCATION, Produced.with(Serdes.String(), Serdes.String()));
        }

        // Start the Kafka Streams producer application
        KafkaStreams producerStreams = new KafkaStreams(producerBuilder.build(), producerProps);
        producerStreams.start();

        // Kafka Streams configuration for the consumer
        Properties consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-consumer-app");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:29092,localhost:39092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka Streams builder for the consumer
        StreamsBuilder consumerBuilder = new StreamsBuilder();

        // Create a consumer stream to consume 100 messages
        consumerBuilder.stream(AppConstant.CAB_LOCATION).foreach((key, value) -> {
            System.out.println("Consumed message: " + value);
        });

        // Start the Kafka Streams consumer application
        KafkaStreams consumerStreams = new KafkaStreams(consumerBuilder.build(), consumerProps);
        consumerStreams.start();

        // Add shutdown hooks to gracefully close the applications
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producerStreams.close();
            consumerStreams.close();
        }));
        return "updated successfully";
    }
}
