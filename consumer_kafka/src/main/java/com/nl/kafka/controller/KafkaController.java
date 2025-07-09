package com.nl.kafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nl.kafka.service.KafkaProducerService;

/**
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:53
 */
@RestController
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    /*
     * Code Ref: spring dependency injection using constructor
     */
    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam String message) {
        kafkaProducerService.sendMessage(message);
        return "Message sent successfully";
    }
    
    @GetMapping("/noOfLitersFilled")
    public String noOfLitersFilled(@RequestParam double filledLiters) {
    	kafkaProducerService.noOfLitersFilled(filledLiters);
    	return "Filled "+ filledLiters +" Liters Successfully";
    }
}