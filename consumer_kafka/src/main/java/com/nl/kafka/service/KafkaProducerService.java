package com.nl.kafka.service;

import com.nl.kafka.entity.KafkaProducerFileMetadata;

public interface KafkaProducerService {

	void sendMessage(String message);

	void noOfLitersFilled(Double litersFilled);

	void producer_save_excel_metadata(KafkaProducerFileMetadata metadata);

	void producer_save_FileMetadata(KafkaProducerFileMetadata metadata);

}
