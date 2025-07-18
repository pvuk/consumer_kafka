package com.nl.kafka.entity;

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
@Entity(name = "BANK_GROUP_MASTER_CR")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class BankGroupMasterCR extends Auditable {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "BANK_GROUP_MASTER_CR_ID_PK")
	private Long bankGroupPMasterCrID;
	@Column(name = "BANK_GROUP_MASTER_ID_FK")
	private Long bankGroupMasterID;//Once record approved, this field should update
	@CsvBindByName(column = "BANK")
	@Column(name = "BANK_NAME", nullable = false, unique = true)
	private String bankName;
	@Column(name = "IS_ACTIVE", columnDefinition = "VARCHAR(1) DEFAULT 'Y'")
	private String isActive;
}
