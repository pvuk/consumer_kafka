package com.nl.kafka.entity;

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
@Entity(name = "BANK_MASTER")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class BankMaster extends Auditable{
	//BANK,IFSC,MICR CODE,BRANCH,ADDRESS,CONTACT,CITY,DISTRICT,STATE
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "BANK_MASTER_ID_PK")
	private Long bankMasterID;
	@Column(name = "BANK_GROUP_MASTER_ID_FK", nullable = false)
	private Long bankGroupMasterID;
	@Column(name = "BANK_NAME", nullable = false)
	private String bankName;
	@Column(name = "IFSC_CODE")
	private String ifscCode;
	@Column(name = "MICR_CODE")
	private String micrCode;
	@Column(name = "BRANCH")
	private String branch;
	@Column(name = "ADDRESS")
	private String address;
	@Column(name = "CONTACT_NUMBER")
	private String contactNumber;
	@Column(name = "MANAGER_NUMBER")
	private String managerNumber;
	@Column(name = "CITY")
	private String city;
	@Column(name = "DISTRICT")
	private String district;
	@Column(name = "STATE")
	private String state;
	@Column(name = "IS_ACTIVE")
	private String isActive;
}
