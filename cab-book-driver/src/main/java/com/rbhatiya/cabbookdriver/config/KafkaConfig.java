package com.rbhatiya.cabbookdriver.config;

import com.rbhatiya.cabbookdriver.constant.AppConstant;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    /**
     * Creating kafka topic
     * @return
     */
    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name(AppConstant.CAB_LOCATION).build();
    }
}
