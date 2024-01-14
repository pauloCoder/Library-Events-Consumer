package com.learnkafka.config;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.List;

@Slf4j
@Setter
@Configuration
@EnableKafka
@ConfigurationProperties(prefix = "spring.kafka.consumer")
public class LibraryEventsconsumerConfig {

    private Long backOffInterval;
    private Long backOffMaxAttempts;

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        var exceptionsToIgnoreList = List.of(
                IllegalArgumentException.class
        );
        var exceptionsToRetryList = List.of(
                RecoverableDataAccessException.class
        );
        // var fixedBackOff = new FixedBackOff(backOffInterval, backOffMaxAttempts - 1);
        var exponentialBackOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(backOffMaxAttempts.intValue() - 1);
        exponentialBackOffWithMaxRetries.setInitialInterval(1000L);
        exponentialBackOffWithMaxRetries.setMultiplier(2.0);
        exponentialBackOffWithMaxRetries.setMaxInterval(2000L);
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
               exponentialBackOffWithMaxRetries
        );
        exceptionsToIgnoreList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        exceptionsToRetryList.forEach(defaultErrorHandler::addRetryableExceptions);
        defaultErrorHandler.setRetryListeners(
                (failedRecord, ex, deliveryAttempt) -> log.error("Failed record in Retry Listener, Exception : {}, deliveryAttempt : {}", ex.getMessage(), deliveryAttempt)
        );
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(defaultErrorHandler);
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

}
