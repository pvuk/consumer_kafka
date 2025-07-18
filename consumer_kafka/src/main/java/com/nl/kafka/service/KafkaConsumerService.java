package com.nl.kafka.service;

public interface KafkaConsumerService {

	void consume(String message);

	void consumeLitersFilled(String message);

}
