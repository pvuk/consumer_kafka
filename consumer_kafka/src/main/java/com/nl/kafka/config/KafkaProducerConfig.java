package com.nl.kafka.config;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.nl.kafka.entity.KafkaProducerFileMetadata;

/**
 * <b>(Kafka Serializer)[https://www.geeksforgeeks.org/java/apache-kafka-serializer-and-deserializer/]</b>
	To understand Kafka Serializer in detail let's first understand the concept of Kafka Producers and Kafka Message Keys. 
	Kafka Producers are going to write data to topics and topics are made of partitions. 
	Now the producers in Kafka will automatically know to which broker and partition to write based on your message and in case there is a Kafka broker failure in your cluster the producers will automatically recover from it which makes Kafka resilient and which makes Kafka so good and used today.</br>

 * @author P.V. UdayKiran
 * @version 1
 * @since created on Wed 18-Jun-2025 12:51
 */
@Configuration
public class KafkaProducerConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT;
	
	//🔹 Template 1: StringSerializer for both key and value
    @Bean
    @Qualifier("stringKafkaTemplate")
    public KafkaTemplate<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /*
         * Here we are using StringSerializer
         */
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
//        return new DefaultKafkaProducerFactory<>(configProps);//Example 1: creating stringKafkaTemplate bean
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
    }
    
    @Bean
    @Qualifier("doubleKafkaTemplate")
    public KafkaTemplate<String, Double> producerFactory_double() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /*
         * Here we are using DoubleSerializer
         */
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
        
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
    }
    
	/**
	 * <b>Producer Serializer</b></br>
		Serializer will indicate how to transform these objects into bytes and they will be used for the key and the value. 
		So say for example that we have the value to be "hello world" and as a string and the key to be "123" and that's an integer. 
		In that case, we need to set the <b>KeySerializer</b> to be an IntegerSerializer / StringSerializer and what this will do internally is that it will convert that integer into bytes, and these bytes will be part of the key which is going to be binary, and the same for the value which is "hello world" as a string. 
		We're going to use a StringSerializer as the <b>ValueSerializer</b> to convert that string into bytes and again this is going to give us our value as part of a binary field. 
	 *
	 */
    @Bean
    @Qualifier(value = "jsonKafkaTemplate_FileMetadata")
    public KafkaTemplate<String, KafkaProducerFileMetadata> producerFactory_FileMetadata() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SPRING_KAFKA_BOOTSTRAP_SERVERS_WITH_PORT);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /*
         * spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
         * Code Ref: Apache kafka does not provide JsonSerializer, so we use it from spring class org.springframework.kafka.support.serializer.JsonSerializer
         */
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        //Code Ref: Prevents deserialization errors due to security restrictions. By default, only java.util and java.lang are trusted.
        String customPackageName = KafkaProducerFileMetadata.class.getCanonicalName();//getCanonicalName: package along class name
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");//adding package to trusted package list
        
        return new KafkaTemplate<String, KafkaProducerFileMetadata>(new DefaultKafkaProducerFactory<>(configProps));
    }

//    @Bean
//    @Qualifier(value = "jsonKafkaTemplate_FileMetadata")
//    public KafkaTemplate<String, KafkaProducerFileMetadata> kafkaTemplate_FileMetadata() {
//        return new KafkaTemplate<>(producerFactory_FileMetadata());
//    }
}