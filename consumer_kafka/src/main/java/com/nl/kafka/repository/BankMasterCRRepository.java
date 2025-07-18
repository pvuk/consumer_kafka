package com.nl.kafka.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.nl.kafka.entity.BankMasterCR;

@Repository
public interface BankMasterCRRepository extends CrudRepository<BankMasterCR, Long> {

	List<BankMasterCR> findAllByIfscCodeNotIn(Collection<String> values);

	@Query(value = "SELECT b.IFSC_CODE FROM BANK_MASTER_CR b WHERE b.BANK_GROUP_MASTER_CR_ID_FK = :bankGroupMasterCrID", nativeQuery = true)
	List<String> findAllByBankMasterCrID(Long bankGroupMasterCrID);

}
