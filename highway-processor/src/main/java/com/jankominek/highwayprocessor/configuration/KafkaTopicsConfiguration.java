package com.jankominek.highwayprocessor.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicsConfiguration {

    @Bean
    public KafkaAdmin.NewTopics kafkaTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("camera-simulator")
                        .partitions(1)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("speeding-alerts")
                        .partitions(1)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("highway-alerts")
                        .partitions(1)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("stolen-vehicles")
                        .partitions(1)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("traffic-jam-alerts")
                        .partitions(1)
                        .replicas(1)
                        .build()
        );
    }
}

