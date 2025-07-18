package com.nl.kafka.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
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
@RequestMapping(value = "/v1")
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    /*
     * Code Ref: spring dependency injection using constructor
     */
    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }
    
	@GetMapping(value = "/versionInfo")
	public ResponseEntity<String> versionInfo() {
		return ResponseEntity.ok("Hello Consumer Kafka");
	}

    @GetMapping("/send")
    public String sendMessage(@RequestParam String message) {
        kafkaProducerService.sendMessage(message);
        return "Message sent successfully";
    }
    
    @GetMapping("/noOfLitersFilled")
    public String noOfLitersFilled(@RequestParam(name = "filledLiters") Double filledLiters) {
    	kafkaProducerService.noOfLitersFilled(filledLiters);
    	return "Filled "+ filledLiters +" Liters Successfully";
    }
}