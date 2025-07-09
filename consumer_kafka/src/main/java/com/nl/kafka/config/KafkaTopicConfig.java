package com.nl.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.nl.kafka.constants.Constants;

/**
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 19:39
 * 
 */
@Configuration
public class KafkaTopicConfig {
	
	//spring bean for kafka topic
	@Bean
	public NewTopic topicFilled() {
		return TopicBuilder.name(Constants.KFKA.TOPIC_NAME.NO_OF_LITERS_FILLED)
				.partitions(1)//no.of partitions allow				.
				.build();
	}
}
