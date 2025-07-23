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
    
    /**
     * Code Ref: Interview Question
     * Q: Calling @GetMapping but doing delete repository operation.
     * Is records deleted ?
     * A: it will delete, but not recommended
     * 
     * 🔍 Why It Works:
		Spring doesn't restrict what logic you can execute inside a controller method. So even if the method is mapped to a GET request, you can call a repository's delete method inside it.
		
		⚠️ Why You Shouldn't Do It:
		RESTful principles: GET is meant to be safe and idempotent, meaning it should not change server state.
		Caching issues: Intermediaries (like browsers or proxies) may cache GET requests, which can lead to unexpected behavior if those requests delete data.
		Security concerns: GET requests can be triggered by simply visiting a URL or clicking a link, which makes accidental or malicious deletions more likely.
		✅ Recommended:
		Use @DeleteMapping for delete operations:
		
		Summary:
		| Annotation       | Purpose               | Safe for Deletes?     |
		|------------------|------------------------|------------------------|
		| `@GetMapping`     | Read-only operations    | ❌ Not recommended     |
		| `@DeleteMapping`  | Deletion operations     | ✅ Yes                 |

     * 
     * @param status
     * @return
     */
    @GetMapping("/deleteByStatus")
    public String deleteByStatus(@RequestParam(name = "status") String status) {
    	int records = kafkaProducerService.deleteByStatus(status);
    	return "Total '"+ records +"' Records Deleted";
    }
}