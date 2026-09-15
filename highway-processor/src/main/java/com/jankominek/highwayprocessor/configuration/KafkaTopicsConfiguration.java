package com.jankominek.highwayprocessor.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfiguration {

    @Bean
    public NewTopic cameraSimulatorTopic() {
        return TopicBuilder.name("camera-simulator")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic speedingAlertsTopic() {
        return TopicBuilder.name("speeding-alerts")
                .partitions(1)
                .replicas(1)
                .build();
    }
}

