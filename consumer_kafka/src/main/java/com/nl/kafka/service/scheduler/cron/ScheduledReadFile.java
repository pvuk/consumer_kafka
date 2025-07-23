package com.nl.kafka.service.scheduler.cron;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.dto.BaseDTO;
import com.nl.kafka.entity.KafkaProducerFileMetadata;
import com.nl.kafka.repository.KafkaProducerFileMetadataRepository;
import com.nl.kafka.service.ExcelToCsvService;
import com.nl.kafka.service.KafkaProducerService;

@EnableScheduling
@Service
public class ScheduledReadFile {

    private final KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository;
	private final Logger LOG = LoggerFactory.getLogger(ScheduledReadFile.class);
	
    private final ExcelToCsvService excelToCsvService;
    private final KafkaProducerService kafkaProducerService;
    //Constructor DI
	public ScheduledReadFile(ExcelToCsvService excelToCsvService, KafkaProducerService kafkaProducerService, KafkaProducerFileMetadataRepository kafkaProducerFileMetadataRepository) {
		this.excelToCsvService = excelToCsvService;
		this.kafkaProducerService = kafkaProducerService;
		this.kafkaProducerFileMetadataRepository = kafkaProducerFileMetadataRepository;
	}
	
	Workbook workbook;
	
	@Scheduled(cron = "0 08 19 * * *")
    public void prepare_excel_files() {
    	String path = "C:\\Users\\venkata.pulipati\\Downloads\\india-ifsc-codes-2-1510j";
    	String timeStamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("ddMMMyyyy_HHmmss_SSS"));
        File inputDir = new File(path);
		File outputDir = new File(path + File.separator + "xls to csv" + File.separator + timeStamp + File.separator);
		
		// Create the output directory if it doesn't exist
        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            if (!created) {
                LOG.error("Failed to create output directory: " + outputDir.getAbsolutePath());
            }
        }
        
        File[] xlsFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".xls"));//reading only .xls extension files 

        if (xlsFiles == null || xlsFiles.length == 0) {
            System.out.println("No XLS files found.");
            return;
        }
        int totalFiles = xlsFiles.length, inProgress = 0, failed = 0;

        for (File xlsFile : xlsFiles) {
        	try {
            	String hostName = InetAddress.getLocalHost().getHostName();
            	String hostAddress = InetAddress.getLocalHost().getHostAddress();
            	
            	KafkaProducerFileMetadata metadata = null;
            	String xlsFileName = null;
                try {
                    xlsFileName = xlsFile.getName();
//                    File csvFile = new File(outputDir, xlsFileName);
                    long size = Files.size(Paths.get(xlsFile.getAbsolutePath()));
                    
                    metadata = KafkaProducerFileMetadata.builder()
//                    		.columnCount()
//                    		.fileId(UUID.randomUUID())//PK Id, it will auto generate
                    		.fileName(xlsFileName)
                    		.fileSizeBytes(size)
                    		.groupID(null)
                    		.ingestionTimestamp(LocalDateTime.now())
//                    		.recordCount(lastRowNum)
                    		.schema(null)
                    		.sourceFilePath(xlsFile.getAbsolutePath())
                    		.sourceSystem(InetAddress.getLocalHost().getHostAddress())
                    		.status(Constants.Status.PENDING)
//                    		.targetFilePath()
                    		.topicID(null)
                    		.build();
                    
                    inProgress++;
                    LOG.info("Reading XLS {}, Completed {} of {}", xlsFileName, totalFiles, inProgress);
                } catch (IOException e) {
                    failed++;
                    LOG.error("Failed to Read XLS {}, Failed {} of {}. Message: {}", xlsFileName, totalFiles, failed, e.getMessage());
//                    e.printStackTrace();
                    
                    metadata = KafkaProducerFileMetadata.builder()
//    							.fileId(UUID.randomUUID())
    							.fileName(xlsFileName)
    							.fileSizeBytes(null)
    							.groupID(null)
    							.ingestionTimestamp(LocalDateTime.now())
    							.recordCount(null)
    							.schema(hostName)
    							.sourceFilePath(xlsFile.getAbsolutePath())
    							.sourceSystem(hostAddress)
    							.status(Constants.Status.FAILED)
    							.targetFilePath(null)
    							.topicID(null)
    							.build();
                }
                
    			kafkaProducerService.producer_save_excel_metadata(metadata);
    		} catch (UnknownHostException e1) {
    			LOG.error("convertAllXlsFiles: UnknownHostException: Message: {}", e1.getMessage());
//    			e1.printStackTrace();
    		}
        }
        
        LOG.info("Total XLS files Read Success: {}, Failed: {} at path: {}", inProgress, failed, outputDir);
    }
	
    // Runs every day at 2 AM (adjust CRON as needed)
    @Scheduled(cron = "0 03 14 * * *")
    public void convert_Xls_To_CSV_Files() {
    	String path = "C:\\Users\\venkata.pulipati\\Downloads\\india-ifsc-codes-2-1510j";
    	String timeStamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("ddMMMyyyy_HHmmss_SSS"));
        File inputDir = new File(path);
		File outputDir = new File(path + File.separator + "xls to csv" + File.separator + timeStamp + File.separator);
		
		// Create the output directory if it doesn't exist
        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            if (!created) {
                LOG.error("Failed to create output directory: " + outputDir.getAbsolutePath());
            }
        }
        
        //FilenameFilter FunctionalInterface method accepts two input values > boolean accept(File dir, String name);
        File[] xlsFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".xls")); 

        if (xlsFiles == null || xlsFiles.length == 0) {
            System.out.println("No XLS files found.");
            return;
        }
        int totalFiles = xlsFiles.length, inProgress = 0, failed = 0;
        for (File xlsFile : xlsFiles) {
        	String csvFileName = null;
            try {
                csvFileName = xlsFile.getName().replace(".xls", ".csv");
                File csvFile = new File(outputDir, csvFileName);
                excelToCsvService.convertXlsToCsv(xlsFile, csvFile);
                inProgress++;
                LOG.info("Converted XLS {} to CSV {}, Completed {} of {}", xlsFile.getName(), csvFile.getName(), totalFiles, inProgress);
            } catch (IOException e) {
                failed++;
                LOG.error("Failed to Convert XLS {} to CSV {}, Failed {} of {}. Message: {}", xlsFile.getName(), csvFileName, totalFiles, failed, e.getMessage());
//                e.printStackTrace();
                
                KafkaProducerFileMetadata metadata = KafkaProducerFileMetadata.builder()
//							.fileId(UUID.randomUUID())
							.fileName(csvFileName)
							.fileSizeBytes(null)
							.groupID(null)
							.ingestionTimestamp(LocalDateTime.now())
							.recordCount(null)
							.sourceFilePath(xlsFile.getAbsolutePath())
							.status(Constants.Status.FAILED)
							.targetFilePath(null)
							.topicID(null)
							.build();
				try {
					metadata.setSchema(InetAddress.getLocalHost().getHostName());
					metadata.setSourceSystem(InetAddress.getLocalHost().getHostAddress());
				} catch (UnknownHostException e1) {
					LOG.error("convertAllXlsFiles: UnknownHostException: Message: {}", e1.getMessage());
					e1.printStackTrace();
				}
				kafkaProducerService.producer_save_FileMetadata(metadata);
            }
        }
        LOG.info("Total XLS files converted to CSV: {} at path: {}", inProgress, outputDir);
    }

	@Scheduled(cron = "10 56 19 * * *")
    public BaseDTO recall_cron_excel_files_by_status() {
    	BaseDTO baseDTO = new BaseDTO();
//    	List<String> statusList = Arrays.asList(Constants.Status.FAILED, Constants.Status.FAILED_SENT_TO_CONSUMER);
    	List<String> statusList = Arrays.asList(Constants.Status.SENT_TO_CONSUMER);//Constants.Status.SENT_TO_CONSUMER
    	List<KafkaProducerFileMetadata> list = kafkaProducerFileMetadataRepository.findAllByStatusIn(statusList);
    	
    	/**
    	 * Code Ref:
    	 * In Java, understanding how arguments are passed to methods is crucial. While the terms "call by value" and "call by reference" are used, Java's mechanism is often described as "pass by value" for both primitive types and object references. 
			<b>Call by Value (for Primitive Data Types):</b>
			When a primitive data type (like int, char, boolean, double, etc.) is passed to a method, a copy of its value is created and passed to the method's parameter.
			Any modifications made to the parameter inside the method do not affect the original variable outside the method. 

			<b>Call by Value (for Object References):</b>
			When an object is passed to a method, a copy of the object's reference (memory address) is passed to the method's parameter.
			This means that both the original variable and the method's parameter now point to the same object in memory.
			Any modifications made to the state of the object (e.g., changing its fields) inside the method will be reflected in the original object outside the method.
			However, if you try to make the parameter reference a different object within the method, this change will not affect the original variable's reference outside the method.
    	 */
    	
    	kafkaProducerService.producer_recall_by_status_excel_metadata(baseDTO, list);//call by value(for Object References)
    	LOG.info("recall_cron_excel_files_by_status: BaseDTO: {}", baseDTO);
    	return baseDTO;
    }
	
	/**
	 * Files not exist in database.
	 * Sometimes failed to read, those are processing here
	 * @return 
	 */
	@Scheduled(cron = "30 24 18 * * *")
    public BaseDTO prepare_non_existing_excel_files() {
		BaseDTO baseDTO = new BaseDTO();
		
    	String path = "C:\\Users\\venkata.pulipati\\Downloads\\india-ifsc-codes-2-1510j";
        File inputDir = new File(path);
		
        String[] extensions = {".xls"};
        List<String> collectFileNames = FileUtils.listFiles(inputDir, extensions, true).stream().map(file -> {
        	int dotIndex = file.getName().lastIndexOf(".");
        	return file.getName().substring(0, dotIndex);
        }).collect(Collectors.toList());
        
        if (collectFileNames == null || collectFileNames.size() == 0) {
            System.out.println("No XLS files found.");
            return baseDTO;
        }
        
        List<KafkaProducerFileMetadata> list = kafkaProducerFileMetadataRepository.findAllByFileNameNotIn(collectFileNames);

    	kafkaProducerService.producer_recall_non_exist_files_in_metadata(baseDTO, list);//call by value(for Object References)
    	LOG.info("prepare_non_existing_excel_files: BaseDTO: {}", baseDTO);
    	return baseDTO;
    }
}
