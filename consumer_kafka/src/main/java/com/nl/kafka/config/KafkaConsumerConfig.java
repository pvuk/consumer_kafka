package com.nl.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.fasterxml.jackson.databind.deser.std.NumberDeserializers.DoubleDeserializer;
import com.nl.kafka.constants.Constants;
import com.nl.kafka.entity.KafkaProducerFileMetadata;

/**
 * <b>Kafka Deserializer</b></br>
 * Reference: https://www.geeksforgeeks.org/java/apache-kafka-serializer-and-deserializer/
 * 
	To understand Kafka Deserializers in detail let's first understand the concept of <b>Kafka Consumers</b>. 
	Kafka Consumers is used to reading data from a topic and remember a topic again is identified by its name. 
	So the consumers are smart enough and they will know which broker to read from and which partitions to read from. 
	And in case of broker failures, the consumers know how to recover and this is again a good property of Apache Kafka. 
	Now data for the consumers is going to be <b>read in order within each partition</b>. Now please refer to the below image. 
	So if we look at a Consumer consuming from <b>Topic-A/Partition-0</b>, then it will first read the message 0, then 1, then 2, then 3, all the way up to message 11. 
	If another consumer is reading from two partitions for example Partition-1 and Partition-2, is going to read both partitions in order. 
	It could be with them at the same time but from within a partition the data is going to be read in order but across partitions, 
	we have no way of saying which one is going to be read first or second and this is why there is <b>no ordering across partitions in Apache Kafka</b>.

 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:44
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT;
	
	/**
	 * So our Kafka consumers are going to be reading our messages from Kafka which are made of bytes 
	 * and so a Deserializer will be needed for the consumer to indicate how to transform these bytes back into some objects 
	 * or data and they will be used on the key and the value of the message.
	 *  
	 * So we have our key and our value and they're both binary fields and bytes 
	 * and so we will use a <b>KeyDeserializer</b> of type IntegerDeserializer / StringDeserializer to transform this into an int 
	 * and get back the number 123 for <b>Key</b> Objects and then we'll use a <b>StringDeserializer</b> to transform the bytes into a string 
	 * and read the <b>value</b> of the object back into the string "hello world". Please refer to the below image. 
	 * 
	 */
    @Bean
    public ConsumerFactory<String, String> consumerFactory_Group_StationID() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_STATION_ID);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory_FS() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory_Group_StationID());
        return factory;
    }
    

    @Bean
    public ConsumerFactory<String, Double> consumerFactory_Group_StationID_double() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_STATION_ID);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Double> kafkaListenerContainerFactory_FS_double() {
        ConcurrentKafkaListenerContainerFactory<String, Double> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory_Group_StationID_double());
        return factory;
    }
    
    /**
     * Code Ref: Error Fix:
     * Caused by: java.lang.IllegalArgumentException: The class 'com.nl.trace.bank.entity.KafkaProducerFileMetadata' is not in the trusted packages: [java.util, java.lang]. 
     * If you believe this class is safe to deserialize, please provide its name. If the serialization is only done by a trusted source, you can also enable trust all (*).
     * 
     * @return
     */
    @Bean
    public ConsumerFactory<String, KafkaProducerFileMetadata> consumerFactory_Group_BanksID() {

//    	JsonDeserializer<KafkaProducerFileMetadata> deserializer = new JsonDeserializer<>(KafkaProducerFileMetadata.class);
//    	String customPackageName = KafkaProducerFileMetadata.class.getPackageName();//Get class package name
//    	deserializer.addTrustedPackages(customPackageName);

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA.CONSUMER.GROUP_ID.KAFKA_GROUP_TRACE_BANK_ID);
//        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        /**
         * Code Ref: Error Fix:
         * Error deserializing VALUE for partition topic-trace-bank-0 at offset 0
			This means Kafka received a message it couldn't deserialize using the configured deserializer. This could be due to:
			
			A mismatch between the producer's serialization format and the consumer's deserialization configuration.
			Corrupted or unexpected data in the topic.
			Missing or incorrect class definitions for deserialization.
			✅ Recommended Fix
			To handle this properly, you should configure an ErrorHandlingDeserializer in your Kafka consumer configuration. 
			This wrapper allows Spring Kafka to catch deserialization errors and route them to the error handler.
         */
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
//        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.your.package"); // or "*" if fully trusted

        //Code Ref: Prevents deserialization errors due to security restrictions. By default, only java.util and java.lang are trusted.
//        String customPackageName = KafkaProducerFileMetadata.class.getPackageName();//Get class package name
//        String customPackageName = KafkaProducerFileMetadata.class.getCanonicalName();//getCanonicalName: package along class name
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");//adding package to trusted package list
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean(name = "kafkaListenerContainerFactory_Trace_Bank")
    public ConcurrentKafkaListenerContainerFactory<String, KafkaProducerFileMetadata> kafkaListenerContainerFactory_Trace_Bank() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaProducerFileMetadata> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory_Group_BanksID());
        return factory;
    }
}