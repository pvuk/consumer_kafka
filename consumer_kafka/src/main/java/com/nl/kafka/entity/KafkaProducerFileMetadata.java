package com.nl.kafka.entity;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * | Column Name           | Data Type           | Description                                                  |
|-----------------------|---------------------|--------------------------------------------------------------|
| `file_id`             | UUID / String       | Unique identifier for the file                               |
| `file_name`           | String              | Name of the CSV file                                         |
| `file_path`           | String              | Full path or URI of the file                                 |
| `file_size_bytes`     | Long                | Size of the file in bytes                                    |
| `record_count`        | Integer             | Number of rows/records in the CSV                            |
| `column_count`        | Integer             | Number of columns in the CSV                                 |
| `schema`              | JSON / String       | Column names and types (if known)                            |
| `ingestion_timestamp` | Timestamp           | When the file was ingested or processed                      |
| `producer_id`         | String              | Identifier of the Kafka producer instance                    |
| `status`              | String              | Status of processing (e.g., SUCCESS, FAILED, IN_PROGRESS)    |
| `error_message`       | String (nullable)   | Error details if processing failed                           |
| `source_system`       | String              | Origin of the file (e.g., system name or external source)    |
| `checksum`            | String              | Hash (e.g., MD5/SHA256) for file integrity verification      |


 */
@Entity
@Table(name = "KAFKA_PRODUCER_FILE_METADATA")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KafkaProducerFileMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "FILE_ID")
    private UUID fileId;

    @Column(name = "FILE_NAME")
    private String fileName;

    @Column(name = "SOURCE_FILE_PATH")
    private String sourceFilePath;
    
    @Column(name = "TARGET_FILE_PATH")
    private String targetFilePath;

    @Column(name = "FILE_SIZE_BYTES")
    private Long fileSizeBytes;

    @Column(name = "RECORD_COUNT")
    private Integer recordCount;

    @Column(name = "COLUMN_COUNT")
    private Integer columnCount;

    @Column(name = "SCHEMA", columnDefinition = "TEXT")
    private String schema;//Column names and types (if known)

    @Column(name = "INGESTION_TIMESTAMP")
    private LocalDateTime ingestionTimestamp;

    @Column(name = "TOPIC_ID")
    private String topicID;
    
    @Column(name = "GROUP_ID")
    private String groupID;

    @Column(name = "STATUS")
    private String status;

    @Column(name = "ERROR_MESSAGE", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "SOURCE_SYSTEM")
    private String sourceSystem;//Origin of the file (e.g., system name or external source) 

//    @Column(name = "CHECKSUM")
//    private String checksum;
    
    @Column(name = "FAILED_TOPIC_ID")
    private String failedTopicID;
    @Column(name = "CONSUMER_RECORD_KEY")
    private String consumerRecordKey;
}