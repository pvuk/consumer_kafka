package com.nl.kafka.config.datasouce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.nl.kafka.constants.Constants;

import jakarta.persistence.EntityManagerFactory;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = "com.nl.kafka.repository",
    entityManagerFactoryRef = "kafka_Parent_Bank_EntityManagerFactory",
    transactionManagerRef = "kafka_Parent_Bank_TransactionManager"
)
public class Kafka_Bank_DataSourceConfig {

	/**
	 * tcp server connection created to connect single db from multiple projects. 
	 * @return
	 */
    @Bean(name = "kafka_Parent_Bank_DataSource")
//    @ConfigurationProperties(prefix = "spring.datasource.trace-cards-db")//getting configuration properties from application.properties
    public DataSource dataSource() {
//        return DataSourceBuilder.create().build();
    	
    	try {
			return DataSourceBuilder.create()
					.url("jdbc:h2:tcp://"+ InetAddress.getLocalHost().getHostAddress() +":"+ Constants.PORT.H2_TCP_PORT +"/D:/data-file-base/h2-db/trace-bank-db;DB_CLOSE_ON_EXIT=FALSE")//JDBC URL
					.username("sa")//DB username
					.password("")//DB password
					.driverClassName("org.h2.Driver")//DB driver
					.build();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
    }

    @Bean
    public JpaVendorAdapter jpaVendorAdapter() {
        HibernateJpaVendorAdapter adapter = new HibernateJpaVendorAdapter();
        adapter.setGenerateDdl(true);
        adapter.setShowSql(true);
        adapter.setDatabase(Database.H2); // Use H2 since you're connecting to H2 DB
        return adapter;
    }

    @Bean
    public EntityManagerFactoryBuilder entityManagerFactoryBuilder(
            JpaVendorAdapter jpaVendorAdapter,
            ObjectProvider<PersistenceUnitManager> persistenceUnitManager) {
        return new EntityManagerFactoryBuilder(jpaVendorAdapter, new HashMap<>(), persistenceUnitManager.getIfAvailable());
    }

    @Bean(name = "kafka_Parent_Bank_EntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("kafka_Parent_Bank_DataSource") DataSource dataSource) {
        return builder
                .dataSource(dataSource)
                .packages("com.nl.kafka.entity")
                .persistenceUnit("bankCards_kafka_project")
                .build();
    }

    @Bean(name = "kafka_Parent_Bank_TransactionManager")
    public PlatformTransactionManager transactionManager(
            @Qualifier("kafka_Parent_Bank_EntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
