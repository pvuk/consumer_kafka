package com.nl.kafka.config.topics;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * reading kafka properties declared in application.properties
 */
@Component
//@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopicProperties {
    private List<String> topics;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
}
