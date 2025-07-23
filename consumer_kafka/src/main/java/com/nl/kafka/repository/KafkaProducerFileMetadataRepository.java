package com.nl.kafka.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.nl.kafka.entity.KafkaProducerFileMetadata;

@Repository
public interface KafkaProducerFileMetadataRepository extends CrudRepository<KafkaProducerFileMetadata, UUID> {

	List<KafkaProducerFileMetadata> findAllByStatusIn(List<String> statusList);

	List<KafkaProducerFileMetadata> findAllByFileNameNotIn(List<String> collectFileNames);

	List<KafkaProducerFileMetadata> findAllByStatus(String status);

	@Transactional
//	@Modifying
	int deleteAllByStatus(String status);

}
