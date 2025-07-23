package com.nl.kafka.service.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.entity.BankGroupMasterCR;
import com.nl.kafka.entity.BankMasterCR;
import com.nl.kafka.entity.KafkaProducerFileMetadata;
import com.nl.kafka.repository.BankGroupMasterCRRepository;
import com.nl.kafka.repository.BankMasterCRRepository;
import com.nl.kafka.repository.KafkaProducerFileMetadataRepository;
import com.nl.kafka.service.KafkaConsumerService;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.MappingStrategy;
/**
 * 
 * @author P.V. UdayKiran	
 * @version 1
 * @since created on Wed 18-Jun-2025 12:52
 */
@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService{
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerServiceImpl.class);
	
    private static final String MY_TOPIC = Constants.KAFKA.TOPIC_NAME.MY_TOPIC;
    private static final String TOPIC_NO_OF_LITERS_FILLED = Constants.KAFKA.TOPIC_NAME.NO_OF_LITERS_FILLED;
//    private static final String TOPIC_NO_OF_LITERS_REMAINING = Constants.KAFKA.TOPIC_NAME.NO_OF_LITERS_REMAINING;
    private static final String TOPIC_TRACE_BANK = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK;

    private KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, KafkaProducerFileMetadata> kafkaTemplate_FileMetadata;
    
//    private final KafkaProducerFileMetadataService kafkaProducerFileMetadataService;
    
    private final BankGroupMasterCRRepository bankGroupMasterCRRepository;
    private final BankMasterCRRepository bankMasterCRRepository;
    private final KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository;
    
	public KafkaConsumerServiceImpl(@Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate
			,
			@Qualifier(value = "jsonKafkaTemplate_FileMetadata") KafkaTemplate<String, KafkaProducerFileMetadata> kafkaTemplate_FileMetadata,
			BankGroupMasterCRRepository bankGroupMasterCRRepository,
			BankMasterCRRepository bankMasterCRRepository,
			KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository
//			,KafkaProducerFileMetadataService kafkaProducerFileMetadataService
			) {
		this.kafkaTemplate = kafkaTemplate;
		this.kafkaTemplate_FileMetadata = kafkaTemplate_FileMetadata;
		this.bankGroupMasterCRRepository = bankGroupMasterCRRepository;
		this.bankMasterCRRepository = bankMasterCRRepository;
		this.kafkaProducerFileMetadataRepository = kafkaProducerFileMetadataRepository;
//		this.kafkaProducerFileMetadataService = kafkaProducerFileMetadataService;
	}
    
//    @KafkaListener(topics = TOPIC, groupId = Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_STATION_ID)
    @KafkaListener(topics = MY_TOPIC, containerFactory = "kafkaListenerContainerFactory_FS")
    @Override
    public void consume(String message) {
        System.out.println("Message received: " + message);
    }
    
//    @KafkaListener(topics = TOPIC_NO_OF_LITERS_FILLED, groupId = Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_STATION_ID)
    @KafkaListener(topics = TOPIC_NO_OF_LITERS_FILLED, containerFactory = "kafkaListenerContainerFactory_FS")
    @Override
    public void consumeLitersFilled(String message) {
    	LOG.info("Received: No.Of Liters Filled: {}", message);
    }

	Workbook workbook;
	
    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager")
    @KafkaListener(topics = Constants.KAFKA.TOPIC_NAME.TOPIC_TRACE_BANK_READ_EXCEL, containerFactory = "kafkaListenerContainerFactory_Trace_Bank")
    public void readExcelAndSavetoDatabase(ConsumerRecord<String, KafkaProducerFileMetadata> record) {
    	KafkaProducerFileMetadata message = record.value();
    	
    	List<String> list = new ArrayList<String>();//Reference: https://javatechonline.com/read-excel-file-in-java-spring-boot-upload-db/

 		// Create a DataFormatter to format and get each cell's value as String
 		DataFormatter dataFormatter = new DataFormatter();

 		// Create the Workbook
 		try {
 			String EXCEL_FILE_PATH = message.getSourceFilePath();
 			workbook = WorkbookFactory.create(new File(EXCEL_FILE_PATH));
 		} catch (EncryptedDocumentException | IOException e) {
 			e.printStackTrace();
 		}
 		
 		String fileName = message.getFileName();
 		
 		// Retrieving the number of sheets in the Workbook
 		LOG.info("-------Workbook : {} has '{}' Sheets-----", fileName, workbook.getNumberOfSheets() );

 		// Getting the Sheet at index zero
 		org.apache.poi.ss.usermodel.Sheet sheet = workbook.getSheetAt(0);

 		// Getting number of columns in the Sheet
 		int noOfColumns = sheet.getRow(0).getLastCellNum();//Gets the index of the last cell contained in this row PLUS ONE
 		int physicalNumberOfCells = sheet.getRow(0).getPhysicalNumberOfCells();//Gets the number of defined cells (NOT number of cells in the actual row!). That is to say if only columns 0,4,5 have values then there would be 3.
 		int noOfNonEmptyHeaderCount = 0;
 		
 		int noOfDataRows = 0;//exclude header columns
 		// Using for-each loop to iterate over the rows and columns
 		for (Row row : sheet) {
 			
 			//TODO remove this lines
 	 		List<String> failedList = Arrays.asList("IFCB2009_130.xls");//, "IFCB2009_101.xls"
 	 		//, "IFCB2009_133.xls" Index 1996 out of bounds for length 1996
 	 		//, "IFCB2009_79.xls"
 	 		//FileName: IFCB2009_54.xls, FileId: 0ea5bcc9-19b1-40b1-bb93-bbef91ff64c9, Message: Index 255 out of bounds for length 17
 	 		//FileName: IFCB2009_97.xls, FileId: 4e28a6c8-87e7-4c6d-a501-da2b9169b93d, Message: Index 38249 out of bounds for length 38249

 	 		if(failedList.contains(fileName)) {
 	 			LOG.error("File "+ fileName +" Reading Failed. Row number: "+ row.getRowNum() +" contains columsn: "+ row.getLastCellNum());
 	 		}
 	 		
 			for (Cell cell : row) {
 				String cellValue = dataFormatter.formatCellValue(cell);
 				if(!cellValue.isEmpty()) {
 	 				list.add(cellValue);
 	 				noOfNonEmptyHeaderCount++;
 				}
 				noOfDataRows++;
 			}
 		}
 		
 		LOG.info("-------Sheet has '"+noOfColumns+"' columns"
 				+ " :: physicalNumberOfCells: '"+ physicalNumberOfCells+"', "
 				+ " noOfNonEmptyHeaderCount: '"+ noOfNonEmptyHeaderCount +", noOfDataRows: '"+ noOfDataRows +"'"
 				+ "------");
 		
 		try {
// 			noOfColumns = noOfNonEmptyHeaderCount;
 	 		// filling excel data and creating list as List<Invoice>
 	 		List<BankMasterCR> bankMasterCRList = createList(list, noOfColumns);
 	 		if(list != null && list.size() > 0) {
 				String bankName = bankMasterCRList.get(0).getBankName();
 				BankGroupMasterCR bankGroupMasterCR = bankGroupMasterCRRepository.findByBankName(bankName);
 				Long bankGroupMasterCrID = null;
 				if(bankGroupMasterCR == null) {
 					bankGroupMasterCR = bankGroupMasterCRRepository.save(BankGroupMasterCR.builder().bankName(bankName).build());
 					LOG.info("FileName: {}, New BankName: '{}' found, saved in BankGroupMasterCr", fileName, bankName);
 				}
 				bankGroupMasterCrID = bankGroupMasterCR.getBankGroupMasterCrID(); 
 				
 				for(BankMasterCR cr : bankMasterCRList) {
 					cr.setBankGroupMasterCrID(bankGroupMasterCrID);
 					cr.setKafkaProducerFileMetadataID(message.getFileId());
 				}
 				Map<Long, List<BankMasterCR>> collect = bankMasterCRList.stream()
 						.collect(Collectors.groupingBy(BankMasterCR::getBankGroupMasterCrID,
 								Collectors.mapping(bankMasterCr -> bankMasterCr, Collectors.toList())));
 				if(collect != null && collect.size() > 0) {
 					for(Map.Entry<Long, List<BankMasterCR>> map : collect.entrySet()) {
 						Long bankGroupMasterCrID2 = map.getKey();
 						List<BankMasterCR> newIfscCodeList = map.getValue();
 						List<String> oldIfscCodeList = bankMasterCRRepository.findAllByBankMasterCrID(bankGroupMasterCrID2);
 						/**
 						 * Code Ref: Filter two list, 1st list ifscode list not exist in 2nd list.
 						 * Saving filter list of new ifsc code data
 						 */
 						List<BankMasterCR> collectSave = newIfscCodeList.stream()
 								.filter(newBankMasterCR -> {
 									return !oldIfscCodeList.contains(newBankMasterCR.getIfscCode());
 								})
 								.collect(Collectors.toList());//Filter duplicate insert
 						
 						LOG.info("FileName: {}, Old Records: {}, New Records: {}, Saving Total Records: {}",
 								fileName,
 								(oldIfscCodeList != null ? oldIfscCodeList.size() : 0),
 								(newIfscCodeList != null ? newIfscCodeList.size() : 0),
 								(collectSave != null ? collectSave.size() : 0));
 						
 						if(collectSave.size() > 0) {
 							bankMasterCRRepository.saveAll(collectSave);
 	 						
 	 						message.setStatus(Constants.Status.SUCCESS);
 						}
 					}
 				} else {
 					LOG.warn("No data available for BankGroupName: {}, FileName: {}", bankGroupMasterCR.getBankName(), fileName);
 				}
 			}
		} catch (Exception e) {
			String errMessage = "Exception occurred while bind excel to entity";
			message.setStatus(Constants.Status.FAILED_TO_PROCESS);
			message.setErrorMessage(errMessage);
			
			LOG.error("readExcelAndSavetoDatabase: Error: {}, FileName: {}, FileId: {}, Message: {}", errMessage, fileName, message.getFileId(), e.getMessage());
		}
 		
 		// Closing the workbook
 		try {
 			workbook.close();
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
         
//        LOG.info("Total XLS files converted to CSV: {} at path: {}", inProgress, outputDir);
    }
    
    @Transactional(transactionManager = "kafka_Parent_Bank_TransactionManager")
    @KafkaListener(topics = TOPIC_TRACE_BANK, containerFactory = "kafkaListenerContainerFactory_Trace_Bank")
    public void listen(ConsumerRecord<String, KafkaProducerFileMetadata> record) {
        try {
            // Your message processing logic
            processMessage(record);
        } catch (Exception e) {

        	/*
        	 * Logging Failed Messages
				Log failed messages to a separate system or file for later analysis. Include:
				
				Message key/value
				Partition and offset
				Timestamp
				Error details
				Example log entry:
				
				[ERROR] Failed to process message at offset 12345: {"user_id": 789, "action": "purchase"} - Error: ValueError('Invalid data format')
        	 */
        	
        	// Log the failure
        	String errorMessage = "Failed to process message";
        	LOG.error("Failed to process message: {}", record.value(), e);
        	
            // Optionally send to a dead letter topic
            sendToDLT(record, "listen:: "+ errorMessage +", Message: "+ e.getMessage());
        }
    }

	private void processMessage(ConsumerRecord<String, KafkaProducerFileMetadata> record) {
		KafkaProducerFileMetadata message = record.value();
		LOG.info("processMessage: Reading file: {}, Topic: {}", message.getFileName(), message.getTopicID());
		
		if (message == null || (message != null && message.getStatus().equals(Constants.Status.FAILED))) {
            
			/*
			 * 🔍 Why Simulate a Failure?
			 * Simulating a failure helps you:

				Test your error handling logic
				Verify that failed messages are retried
				Ensure messages are sent to the Dead Letter Topic (DLT)
				Confirm logging and monitoring are working
			 */
			String errorMessage = "Simulated failure";
			sendToDLT(record, "listen:: "+ errorMessage +", No data available to process");// send to a dead letter topic
            
			throw new RuntimeException("Simulated failure");
		}
		
		// Process message normally
		String targetFilePath = message.getTargetFilePath();
		try {
			Reader reader = new BufferedReader(new FileReader(targetFilePath));
			List<BankMasterCR> list = null;
//			List<BankMasterCR> list = 
//					new CsvToBeanBuilder<BankMasterCR>(new FileReader(targetFilePath))
//			  .withType(BankMasterCR.class)
//			  .withSeparator(',')
//			  .withIgnoreLeadingWhiteSpace(true)
//			  .withIgnoreEmptyLine(true)
//			  .build()
//			  .parse();
			

//			CSVReaderBuilder readerBuilder = new CSVReaderBuilder(reader)
//					.withCSVParser(new CSVParserBuilder().withSeparator('|').build());

			CsvToBean<BankMasterCR> csvToBean = new CsvToBeanBuilder<BankMasterCR>(reader)
			  .withType(BankMasterCR.class)
			  .withSeparator('|')
//			  .withIgnoreLeadingWhiteSpace(true)
//			  .withIgnoreEmptyLine(true)
			  .build();
			
			MappingStrategy<BankMasterCR> mappingStrategy = new HeaderColumnNameMappingStrategy<>();
			mappingStrategy.setType(BankMasterCR.class);
			csvToBean.setMappingStrategy(mappingStrategy);
			
			list = csvToBean.parse();
			
			
			LOG.info("Processing: {}, Total Size: {}", message.toString(), (list != null ? list.size() : 0));
			
			if(list != null && list.size() > 0) {
				String bankName = list.get(0).getBankName();
				BankGroupMasterCR bankGroupMasterCR = bankGroupMasterCRRepository.findByBankName(bankName);
				if(bankGroupMasterCR != null) {
					bankGroupMasterCR = bankGroupMasterCRRepository.save(BankGroupMasterCR.builder().bankName(bankName).build());

					Long bankGroupMasterCrID = bankGroupMasterCR.getBankGroupMasterCrID(); 
					
					list.forEach(cr -> {
						cr.setBankGroupMasterCrID(bankGroupMasterCrID);
						cr.setKafkaProducerFileMetadataID(message.getFileId());
					});
					Map<Long, List<BankMasterCR>> collect = list.stream()
							.collect(Collectors.groupingBy(BankMasterCR::getBankGroupMasterCrID,
									Collectors.mapping(bankMasterCr -> bankMasterCr, Collectors.toList())));
					if(collect != null && collect.size() > 0) {
						for(Map.Entry<Long, List<BankMasterCR>> map : collect.entrySet()) {
							Long bankGroupMasterCrID2 = map.getKey();
							List<BankMasterCR> newIfscCodeList = map.getValue();
							List<String> oldIfscCodeList = bankMasterCRRepository.findAllByBankMasterCrID(bankGroupMasterCrID2);
							/**
							 * Code Ref: Filter two list, 1st list ifscode list not exist in 2nd list.
							 * Saving filter list of new ifsc code data
							 */
							List<BankMasterCR> collectSave = newIfscCodeList.stream().filter(newBankMasterCR -> !oldIfscCodeList.contains(newBankMasterCR.getIfscCode())).collect(Collectors.toList());
							bankMasterCRRepository.saveAll(collectSave);						
						}
					} else {
						LOG.warn("No data available for BankGroupName: {}, FileName: {}", bankGroupMasterCR.getBankName(), message.getFileName());
					}
				} else {
					LOG.error("Binding data to entity failed. FileName: {}, Path: {}", message.getFileName(), message.getTargetFilePath());
				}
				
			}
			
		} catch (IllegalStateException e) {
			String errorMessage = "processMessage: IllegalStateException occurred while reading FilePath: "+ targetFilePath +", Message: "+ e.getMessage();
			LOG.error(errorMessage);
			sendToDLT(record, errorMessage);
			return;
//			e.printStackTrace();
		} catch (FileNotFoundException e) {
			String errorMessage = "processMessage: FileNotFoundException occurred while reading FilePath: "+ targetFilePath +", Message: "+ e.getMessage();
			LOG.error(errorMessage);
			sendToDLT(record, errorMessage);
			return;
//			e.printStackTrace();
		}
		LOG.info("Processed message successfully: {}", message);
		message.setStatus(Constants.Status.SUCCESS);
//		kafkaProducerFileMetadataService.saveFileMetadata(message);
		kafkaProducerFileMetadataRepository.save(message);
	}

    /**
     * Use Dead Letter Queue (DLQ)
		Set up a Dead Letter Topic in Kafka to send failed messages for further inspection.
		
		Configure your consumer to produce failed messages to a DLQ.
		Monitor and analyze the DLQ regularly.

     * @param record
     * @param errorMessage 
     */
    public void sendToDLT(ConsumerRecord<String, KafkaProducerFileMetadata> record, String errorMessage) {
    	KafkaProducerFileMetadata kafkaProducerFileMetadata = record.value();
    	String failedTopicID = "your-dead-letter-topic";
    	String consumerRecordKey = record.key();
    	
    	kafkaProducerFileMetadata.setStatus(Constants.Status.FAILED);
    	kafkaProducerFileMetadata.setErrorMessage(errorMessage);
    	kafkaProducerFileMetadata.setFailedTopicID(failedTopicID);
    	kafkaProducerFileMetadata.setConsumerRecordKey(consumerRecordKey);
//    	kafkaProducerFileMetadataService.saveFileMetadata(kafkaProducerFileMetadata);
    	kafkaProducerFileMetadataRepository.save(kafkaProducerFileMetadata);
    	
    	kafkaTemplate_FileMetadata.send(failedTopicID, consumerRecordKey, kafkaProducerFileMetadata);
    }
    
	
	private List<BankMasterCR> createList(List<String> excelData, int noOfColumns) {

		ArrayList<BankMasterCR> invList = new ArrayList<BankMasterCR>();

		int i = noOfColumns;
		do {
			BankMasterCR inv = new BankMasterCR();// BANK,IFSC,MICR CODE,BRANCH,ADDRESS,CONTACT,CITY,DISTRICT,STATE

			inv.setBankName(excelData.get(i));
			inv.setIfscCode(excelData.get(i + 1));
			inv.setMicrCode(excelData.get(i + 2));
			inv.setBranch(excelData.get(i + 3));
			inv.setAddress(excelData.get(i + 4));
			inv.setContactNumber(excelData.get(i + 5));
			inv.setCity(excelData.get(i + 6));
			inv.setDistrict(excelData.get(i + 7));
			inv.setState(excelData.get(i + 8));

			invList.add(inv);
			i = i + (noOfColumns);

		} while (i < excelData.size());
		return invList;
	}
	
    //TODO
    /*
     * Monitoring and Metrics
		Use tools like:
		
		Prometheus + Grafana for metrics
		Kafka Manager / Confluent Control Center for topic and consumer monitoring
		ELK Stack (Elasticsearch, Logstash, Kibana) for log analysis
		Track metrics like:
		
		Number of failed messages
		Failure rate per topic/partition
		Error types
		5. Retry Mechanism
		Implement a retry strategy:
		
		Immediate retry (e.g., 3 attempts)
		Delayed retry with backoff
		Move to DLQ after max retries
     */
}