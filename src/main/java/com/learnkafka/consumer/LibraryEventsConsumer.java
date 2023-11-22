package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsConsumer {

    @KafkaListener(topics = "${spring.kafka.topic}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("ConsumerRecord : {}", consumerRecord);
    }

}