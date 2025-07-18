package com.nl.kafka.service;

import java.io.File;
import java.io.IOException;

public interface ExcelToCsvService {

	void convertXlsToCsv(File xlsFile, File csvFile) throws IOException;

}
