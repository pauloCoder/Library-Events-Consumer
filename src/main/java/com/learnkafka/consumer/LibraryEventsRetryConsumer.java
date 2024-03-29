package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsRetryConsumer {

    private final LibraryEventsService libraryEventsService;

    @KafkaListener(topics = "${topics.retry.listener}", autoStartup = "${topics.retry.startup: true}", groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord in Retry Consumer : {}", consumerRecord);
         libraryEventsService.processLibraryEvent(consumerRecord);
    }

}
