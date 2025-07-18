package com.nl.kafka.service.scheduler.cron;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.entity.KafkaProducerFileMetadata;
import com.nl.kafka.service.ExcelToCsvService;
import com.nl.kafka.service.KafkaProducerService;

@EnableScheduling
@Service
public class ScheduledReadFile {
	private final Logger LOG = LoggerFactory.getLogger(ScheduledReadFile.class);
	
    private final ExcelToCsvService excelToCsvService;
    private final KafkaProducerService kafkaProducerService;
    //Constructor DI
	public ScheduledReadFile(ExcelToCsvService excelToCsvService, KafkaProducerService kafkaProducerService) {
		this.excelToCsvService = excelToCsvService;
		this.kafkaProducerService = kafkaProducerService;
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
}
