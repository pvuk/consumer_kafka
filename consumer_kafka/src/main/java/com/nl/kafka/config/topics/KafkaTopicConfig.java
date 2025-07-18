package com.nl.kafka.config.topics;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 19:39
 * 
 */
@Configuration
public class KafkaTopicConfig {

	private final KafkaTopicProperties kafkaProperties;

    public KafkaTopicConfig(KafkaTopicProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }
    
    /**
     * Reading multiple topics from kafkaProperties (application.properties)
     * 
     * @return
     */
    @Bean
    public List<NewTopic> kafkaTopics() {
        return kafkaProperties.getTopics().stream()
                .map(name -> TopicBuilder.name(name)
                        .partitions(1)
                        .replicas(1)
                        .build())
                .collect(Collectors.toList());
    }

    //spring bean for kafka topic
//	@Bean
//	public NewTopic topic_NO_OF_LITERS_FILLED() {
//		return TopicBuilder.name(Constants.KAFKA.TOPIC_NAME.NO_OF_LITERS_FILLED)
//				.partitions(1)//no.of partitions allow				.
//				.build();
//	}
//	
//	@Bean
//	public NewTopic topic_MY_TOPIC() {
//		return TopicBuilder.name(Constants.KAFKA.TOPIC_NAME.MY_TOPIC)
//				.partitions(1)//no.of partitions allow				.
//				.build();
//	}

}
