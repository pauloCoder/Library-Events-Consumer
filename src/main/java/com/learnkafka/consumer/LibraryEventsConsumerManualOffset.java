package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Slf4j
//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = "${spring.kafka.topic}")
    public void onMessage(@NonNull ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("[LibraryEventsConsumerManualOffset] ConsumerRecord : {}", consumerRecord);
        if (acknowledgment == null) {
            throw new IllegalArgumentException("Acknowledgment should not be null");
        }
        acknowledgment.acknowledge();
    }

}
