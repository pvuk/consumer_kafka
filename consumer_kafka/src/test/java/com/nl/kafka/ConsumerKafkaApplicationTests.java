package com.nl.kafka;

import org.springframework.boot.test.context.SpringBootTest;

/**
 * <b>Testing Spring Boot Components</b>
	Spring Boot provides built-in support for testing various components such as controllers, services, and repositories. Use the @SpringBootTest annotation to load the entire application context, or use @WebMvcTest, @DataJpaTest, and @RestClientTest for testing specific components:

 */
@SpringBootTest(classes = ConsumerKafkaApplication.class)
class ConsumerKafkaApplicationTests {

//	@Test
//	void contextLoads() {
//	}

}
