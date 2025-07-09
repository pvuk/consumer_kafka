package com.nl.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.nl.kafka.constants.Constants;
/**
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:52
 */
@Service
public class KafkaConsumerService {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerService.class);
	
    private static final String TOPIC = Constants.KFKA.TOPIC_NAME.MY_TOPIC;
    private static final String TOPIC_NO_OF_LITERS_FILLED = Constants.KFKA.TOPIC_NAME.NO_OF_LITERS_FILLED;
    private static final String TOPIC_NO_OF_LITERS_REMAINING = Constants.KFKA.TOPIC_NAME.NO_OF_LITERS_REMAINING;
    
    @KafkaListener(topics = TOPIC, groupId = Constants.KFKA.GROUP_ID.KAFKA_GROUP_STATION_ID)
    public void consume(String message) {
        System.out.println("Message received: " + message);
    }
    
    @KafkaListener(topics = TOPIC_NO_OF_LITERS_FILLED, groupId = Constants.KFKA.GROUP_ID.KAFKA_GROUP_STATION_ID)
    public void consumeLitersFilled(String message) {
    	LOG.info("Received: No.Of Liters Filled: {}", message);
    }
}