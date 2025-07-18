package com.nl.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.nl.kafka.app.initializer.EarlyInitializer;
/**
 * Reference: https://www.geeksforgeeks.org/advance-java/spring-boot-integration-with-kafka/
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:42
 */
@SpringBootApplication
public class ConsumerKafkaApplication {
	
	/**
	 * Before run ConsumerKafkaApplication.java
	 * 
	 * you need to run below commands. Open cmd
	 * 
	 * Step 1: Run zookeper server
	 * D:\AppData\kafka_2.12-3.9.1>D:\AppData\kafka_2.12-3.9.1\bin\windows\zookeeper-server-start.bat D:\AppData\kafka_2.12-3.9.1\config\zookeeper.properties
	 * 
	 * Step 2: Run Kafka server
	 * D:\AppData\kafka_2.12-3.9.1>.\bin\windows\kafka-server-start.bat .\config\server.properties
	 */
	public static void main(String[] args) {
//		SpringApplication.run(ConsumerKafkaApplication.class, args);
		SpringApplication springApplication = new SpringApplication(ConsumerKafkaApplication.class);
		springApplication.addInitializers(new EarlyInitializer());
		springApplication.run(args);
	}

}
