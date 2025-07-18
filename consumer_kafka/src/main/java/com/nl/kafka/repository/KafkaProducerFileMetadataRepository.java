package com.nl.kafka.repository;

import java.util.UUID;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.nl.kafka.entity.KafkaProducerFileMetadata;

@Repository
public interface KafkaProducerFileMetadataRepository extends CrudRepository<KafkaProducerFileMetadata, UUID> {

}
