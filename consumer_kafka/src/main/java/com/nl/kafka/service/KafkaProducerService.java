package com.nl.kafka.service;

import java.util.List;

import com.nl.kafka.dto.BaseDTO;
import com.nl.kafka.entity.KafkaProducerFileMetadata;

public interface KafkaProducerService {

	void sendMessage(String message);

	void noOfLitersFilled(Double litersFilled);

	void producer_save_excel_metadata(KafkaProducerFileMetadata metadata);

	void producer_save_FileMetadata(KafkaProducerFileMetadata metadata);

	void producer_recall_by_status_excel_metadata(BaseDTO baseDTO, List<KafkaProducerFileMetadata> statusList);

	void producer_recall_non_exist_files_in_metadata(BaseDTO baseDTO, List<KafkaProducerFileMetadata> list);

	int deleteByStatus(String status);

}
