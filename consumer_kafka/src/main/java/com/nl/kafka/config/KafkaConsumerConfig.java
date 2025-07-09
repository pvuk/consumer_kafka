package com.nl.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.nl.kafka.constants.Constants;

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
	
	/**
	 * So our Kafka consumers are going to be reading our messages from Kafka which are made of bytes and so a Deserializer will be needed for the consumer to indicate how to transform these bytes back into some objects or data and they will be used on the key and the value of the message. 
	 * So we have our key and our value and they're both binary fields and bytes and so we will use a <b>KeyDeserializer</b> of type IntegerDeserializer / StringDeserializer to transform this into an int and get back the number 123 for <b>Key</b> Objects and then we'll use a <b>StringDeserializer</b> to transform the bytes into a string and read the <b>value</b> of the object back into the string "hello world". Please refer to the below image. 
	 * 
	 */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KFKA.GROUP_ID.KAFKA_GROUP_STATION_ID);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}