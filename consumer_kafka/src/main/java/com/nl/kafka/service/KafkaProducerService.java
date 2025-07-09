package com.nl.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.nl.kafka.constants.Constants;

/**
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:52
 */
@Service
public class KafkaProducerService {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerService.class);
	
    private static final String TOPIC = Constants.KFKA.TOPIC_NAME.MY_TOPIC;
    private static final String TOPIC_NO_OF_LITERS_FILLED = Constants.KFKA.TOPIC_NAME.NO_OF_LITERS_FILLED;
    private static final String TOPIC_NO_OF_LITERS_REMAINING = Constants.KFKA.TOPIC_NAME.NO_OF_LITERS_REMAINING;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        kafkaTemplate.send(TOPIC, message);
        System.out.println("Message sent: " + message);
    }
    
    public void noOfLitersFilled(Double litersFilled) {
    	Message<Double> message = MessageBuilder.withPayload(litersFilled)
    			.setHeader(KafkaHeaders.TOPIC, TOPIC_NO_OF_LITERS_FILLED)
    			.build();
    	kafkaTemplate.send(message);
    	LOG.info(String.format("Sent: No.Of Liters Filled: %s", message.toString()));
    }
}