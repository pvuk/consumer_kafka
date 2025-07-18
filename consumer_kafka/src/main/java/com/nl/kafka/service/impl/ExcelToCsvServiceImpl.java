package com.nl.kafka.service.impl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.entity.KafkaProducerFileMetadata;
import com.nl.kafka.service.ExcelToCsvService;
import com.nl.kafka.service.KafkaProducerService;

@Service
public class ExcelToCsvServiceImpl implements ExcelToCsvService {

	private final KafkaProducerService kafkaProducerService;
	public ExcelToCsvServiceImpl(KafkaProducerService kafkaProducerService) {
		this.kafkaProducerService = kafkaProducerService;
	}
	
	@Override
    public void convertXlsToCsv(File xlsFile, File csvFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(xlsFile);
             Workbook workbook = new HSSFWorkbook(fis);
             FileWriter writer = new FileWriter(csvFile)) {

            Sheet sheet = workbook.getSheetAt(0);

            for (Row row : sheet) {
                StringBuilder rowData = new StringBuilder();
                for (Cell cell : row) {
                    rowData.append(getCellValue(cell)).append("|");//delimiter for each column separate
                }
                writer.write(rowData.toString().replaceAll(",$", "") + "\n");
            }
            int rows = sheet.getPhysicalNumberOfRows();//Total Rows
            int lastRowNum = sheet.getLastRowNum();//Exclude Header
            long size = Files.size(Paths.get(csvFile.getAbsolutePath()));
//            if(size > 0) {
//                List<BankMasterCR> bankList = new CsvToBeanBuilder<BankMasterCR>(new FileReader(csvFile))
//                        .withType(BankMasterCR.class)
//                        .withIgnoreLeadingWhiteSpace(true)
//                        .build()
//                        .parse();
//            }
            KafkaProducerFileMetadata metadata = KafkaProducerFileMetadata.builder()
//            		.columnCount(sheet.rowIterator().next().getPhysicalNumberOfCells())
//            		.fileId(UUID.randomUUID())//PK Id, it will auto generate
            		.fileName(csvFile.getName())
            		.fileSizeBytes(size)
            		.groupID(null)
            		.ingestionTimestamp(LocalDateTime.now())
            		.recordCount(lastRowNum)
            		.schema(null)
            		.sourceFilePath(xlsFile.getAbsolutePath())
            		.sourceSystem(InetAddress.getLocalHost().getHostAddress())
            		.status(Constants.Status.PENDING)
            		.targetFilePath(csvFile.getAbsolutePath())
            		.topicID(null)
            		.build();
            kafkaProducerService.producer_save_FileMetadata(metadata);
        }
    }

    private String getCellValue(Cell cell) {
        switch (cell.getCellType()) {
            case STRING: return cell.getStringCellValue();
            case NUMERIC: return String.valueOf(cell.getNumericCellValue());
            case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
            case FORMULA: return cell.getCellFormula();
            case BLANK: return "";
            default: return "";
        }
    }
}
