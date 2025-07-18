package com.nl.kafka.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.entity.KafkaProducerFileMetadata;
import com.nl.kafka.repository.KafkaProducerFileMetadataRepository;
import com.nl.kafka.service.KafkaProducerService;

/**
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:52
 */
@Service
public class KafkaProducerServiceImpl implements KafkaProducerService{
	private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);
	
    private static final String MY_TOPIC = Constants.KAFKA.TOPIC_NAME.MY_TOPIC;
    private static final String TOPIC_NO_OF_LITERS_FILLED = Constants.KAFKA.TOPIC_NAME.NO_OF_LITERS_FILLED;
//    private static final String TOPIC_NO_OF_LITERS_REMAINING = Constants.KAFKA.TOPIC_NAME.NO_OF_LITERS_REMAINING;

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, Double> kafkaTemplate_Double_Serializer;
    private final KafkaTemplate<String, KafkaProducerFileMetadata> kafkaTemplate_FileMetadata;
    private final KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository;

    public KafkaProducerServiceImpl(
    		@Qualifier(value = "stringKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
    		@Qualifier(value = "doubleKafkaTemplate") KafkaTemplate<String, Double> kafkaTemplate_Double_Serializer,
    		KafkaTemplate<String, KafkaProducerFileMetadata> kafkaTemplate_FileMetadata,
    		KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository) {
    	
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate_Double_Serializer = kafkaTemplate_Double_Serializer;
        this.kafkaTemplate_FileMetadata = kafkaTemplate_FileMetadata;
        this.kafkaProducerFileMetadataRepository = kafkaProducerFileMetadataRepository;
        
    }

    @Override
    public void sendMessage(String message) {
        kafkaTemplate.send(MY_TOPIC, message);
        System.out.println("Message sent: " + message);
    }
    
    @Override
    public void noOfLitersFilled(Double litersFilled) {
    	Message<Double> message = MessageBuilder.withPayload(litersFilled)
    			.setHeader(KafkaHeaders.TOPIC, TOPIC_NO_OF_LITERS_FILLED)
    			.build();
    	kafkaTemplate_Double_Serializer.send(message);
    	LOG.info(String.format("Sent: No.Of Liters Filled: %s", message.toString()));
    }

    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager")
	@Override
    public void producer_save_excel_metadata(KafkaProducerFileMetadata metadata) {
    	String TOPIC_TRACE_BANK_READ_EXCEL = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK_READ_EXCEL;
    	metadata.setTopicID(TOPIC_TRACE_BANK_READ_EXCEL);
    	metadata.setGroupID(Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_TRACE_BANK_ID);
    	Message<KafkaProducerFileMetadata> message = MessageBuilder.withPayload(metadata)
    			.setHeader(KafkaHeaders.TOPIC, TOPIC_TRACE_BANK_READ_EXCEL)
    			.build();
    	
    	metadata = kafkaProducerFileMetadataRepository.save(metadata);
    	String infoMetadata = "FileId: "+ metadata.getFileId() +", FileName: "+ metadata.getFileName();
    	
    	kafkaTemplate_FileMetadata.send(message);
    	LOG.info(String.format("Sent: consumer-kafka: Saved Metadata: Info: %s, Total Records: %s", infoMetadata, metadata.getRecordCount()));
    }
    
    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager"
//			, propagation = Propagation.REQUIRED
			)
	@Override
    public void producer_save_FileMetadata(KafkaProducerFileMetadata metadata) {
    	String TOPIC_TRACE_BANK = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK;
    	metadata.setTopicID(TOPIC_TRACE_BANK);
    	metadata.setGroupID(Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_TRACE_BANK_ID);
    	Message<KafkaProducerFileMetadata> message = MessageBuilder.withPayload(metadata)
    			.setHeader(KafkaHeaders.TOPIC, TOPIC_TRACE_BANK)
    			.build();
    	
    	metadata = kafkaProducerFileMetadataRepository.save(metadata);
    	String infoMetadata = "FileId: "+ metadata.getFileId() +", FileName: "+ metadata.getFileName();
    	
    	kafkaTemplate_FileMetadata.send(message);
    	LOG.info(String.format("Sent: trace-data-seeder: Saved Metadata: Info: %s, Total Records: %s", infoMetadata, metadata.getRecordCount()));
    }
}