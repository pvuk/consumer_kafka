package com.nl.kafka.service.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.dto.BaseDTO;
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
    
    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager")
	@Override
    public void producer_recall_by_status_excel_metadata(BaseDTO baseDTO, List<KafkaProducerFileMetadata> statusList) {

		String TOPIC_TRACE_BANK_READ_EXCEL = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK_READ_EXCEL;
    	
    	Map<String,Object> configurationProperties = kafkaTemplate_FileMetadata.getProducerFactory().getConfigurationProperties();
    	Producer<String, KafkaProducerFileMetadata> producer = new KafkaProducer<>(configurationProperties);

    	int totalFiles = statusList.size();
    	AtomicInteger sent = new AtomicInteger(), failed = new AtomicInteger();
    	try {

        	for(KafkaProducerFileMetadata metadata : statusList) {

    			UUID fileId = metadata.getFileId();
            	try {
            		ProducerRecord<String, KafkaProducerFileMetadata> producerRecord = new ProducerRecord<String, KafkaProducerFileMetadata>(
            				TOPIC_TRACE_BANK_READ_EXCEL, metadata);
                	
            		producer.send(producerRecord, (recordMetadata, exception) -> {
            			KafkaProducerFileMetadata value = producerRecord.value();
            			String fileName = value.getFileName();
            			
            			if(exception == null) {
            				value.setStatus(Constants.Status.PENDING);
                           
            				LOG.info("producer_recall_by_status_excel_metadata: Reading XLS {}, Completed {} of {}", fileName, totalFiles, sent.getAndIncrement());
            			} else {
            				value.setStatus(Constants.Status.FAILED_SENT_TO_CONSUMER);

            				LOG.error("producer_recall_by_status_excel_metadata: Failed to Read XLS {}, FileId: {}, Failed {} of {}. Message: {}",
    								fileName, value.getFileId(), totalFiles, failed, exception.getMessage());
            			}
            			value = kafkaProducerFileMetadataRepository.save(value);
            	    	String infoMetadata = "FileId: "+ value.getFileId() +", FileName: "+ value.getFileName();
            	    	LOG.info(String.format("Producer Status: {}, Saved Metadata: Info: %s, Total Records: %s", value.getStatus(), infoMetadata, value.getRecordCount()));
            		});
        		} catch (Exception e) {
        			LOG.error("producer_recall_by_status_excel_metadata: FieldId: {}, Message: {}", fileId, e.getMessage());
        		}
        	}//END loop
		} finally {
			producer.flush();
			producer.close();
			
			String message = "Total Files: "+ totalFiles +", Sent: "+ sent.get() +", Failed: "+ failed.get();
			baseDTO.setStatusMessage(message);
			baseDTO.setStatusCode((failed.get() == 0) ? HttpStatus.OK.value() : HttpStatus.BAD_REQUEST.value());
		}
		
    }
    

    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager")
	@Override
    public void producer_recall_non_exist_files_in_metadata(BaseDTO baseDTO, List<KafkaProducerFileMetadata> statusList) {
    	
    	String methodName = "producer_recall_non_exist_files_in_metadata";
    	
		String TOPIC_TRACE_BANK_READ_EXCEL = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK_READ_EXCEL;
    	
    	Map<String,Object> configurationProperties = kafkaTemplate_FileMetadata.getProducerFactory().getConfigurationProperties();
    	Producer<String, KafkaProducerFileMetadata> producer = new KafkaProducer<>(configurationProperties);

    	int totalFiles = statusList.size();
    	AtomicInteger sent = new AtomicInteger(), failed = new AtomicInteger();
    	try {

        	for(KafkaProducerFileMetadata metadata : statusList) {

    			UUID fileId = metadata.getFileId();
            	try {
            		ProducerRecord<String, KafkaProducerFileMetadata> producerRecord = new ProducerRecord<String, KafkaProducerFileMetadata>(
            				TOPIC_TRACE_BANK_READ_EXCEL, metadata);
                	
            		producer.send(producerRecord, (recordMetadata, exception) -> {
            			KafkaProducerFileMetadata value = producerRecord.value();
            			String fileName = value.getFileName();
            			
            			if(exception == null) {
            				value.setStatus(Constants.Status.SENT_TO_CONSUMER);
                           
            				LOG.info("{}: Reading XLS {}, Completed {} of {}", methodName, fileName, totalFiles, sent.getAndIncrement());
            			} else {
            				value.setStatus(Constants.Status.FAILED_SENT_TO_CONSUMER);

            				LOG.error("{}: Failed to Read XLS {}, FileId: {}, Failed {} of {}. Message: {}",
            						methodName,
    								fileName, value.getFileId(), totalFiles, failed, exception.getMessage());
            			}
            			value = kafkaProducerFileMetadataRepository.save(value);
            	    	String infoMetadata = "FileId: "+ value.getFileId() +", FileName: "+ value.getFileName();
            	    	LOG.info(String.format("Producer Status: {}, Saved Metadata: Info: %s, Total Records: %s", value.getStatus(), infoMetadata, value.getRecordCount()));
            		});
        		} catch (Exception e) {
        			LOG.error("{}: FieldId: {}, Message: {}", methodName, fileId, e.getMessage());
        		}
        	}//END loop
		} finally {
			producer.flush();
			producer.close();
			
			String message = "Total Files: "+ totalFiles +", Sent: "+ sent.get() +", Failed: "+ failed.get();
			LOG.info("{}, {}", methodName, message);
			baseDTO.setStatusMessage(message);
			baseDTO.setStatusCode((failed.get() == 0) ? HttpStatus.OK.value() : HttpStatus.BAD_REQUEST.value());
		}
		
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
    
	@Override
	public int deleteByStatus(String status) {
		List<KafkaProducerFileMetadata> list = kafkaProducerFileMetadataRepository.findAllByStatus(status);
		int stat = kafkaProducerFileMetadataRepository.deleteAllByStatus(status);
		return stat == 0 ? 0 : list.size();
	}
}