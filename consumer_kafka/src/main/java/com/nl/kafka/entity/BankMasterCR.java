package com.nl.kafka.entity;

import java.util.UUID;

import com.opencsv.bean.CsvBindByName;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Table
@Entity(name = "BANK_MASTER_CR")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class BankMasterCR extends Auditable{
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "BANK_MASTER_CR_ID_PK")
	private Long bankMasterCrID;
	
	@Column(name = "BANK_MASTER_ID_FK")
	private Long bankMasterID;
	
	@Column(name = "BANK_GROUP_MASTER_CR_ID_FK", nullable = false)
	private Long bankGroupMasterCrID;
	
	@CsvBindByName(column = "BANK")
	@Column(name = "BANK_NAME", nullable = false)
	private String bankName;
	@CsvBindByName(column = "IFSC")
	@Column(name = "IFSC_CODE")
	private String ifscCode;
	@CsvBindByName(column = "MICR CODE")
	@Column(name = "MICR_CODE")
	private String micrCode;
	@CsvBindByName(column = "BRANCH")
	@Column(name = "BRANCH")
	private String branch;
	@CsvBindByName(column = "ADDRESS")
	@Column(name = "ADDRESS")
	private String address;
	@CsvBindByName(column = "CONTACT")
	@Column(name = "CONTACT_NUMBER")
	private String contactNumber;
	@Column(name = "MANAGER_NUMBER")
	private String managerNumber;
	@CsvBindByName(column = "CITY")
	@Column(name = "CITY")
	private String city;
	@CsvBindByName(column = "DISTRICT")
	@Column(name = "DISTRICT")
	private String district;
	@CsvBindByName(column = "STATE")
	@Column(name = "STATE")
	private String state;
	
	@Column(name = "KAFKA_PRODUCER_FILE_METADATA_ID")
	private UUID kafkaProducerFileMetadataID;
}
