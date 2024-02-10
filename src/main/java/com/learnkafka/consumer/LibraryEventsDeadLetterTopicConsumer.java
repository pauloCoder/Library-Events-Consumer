package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsDeadLetterTopicConsumer {

    @KafkaListener(topics = "${topics.dlt.listener}", autoStartup = "${topics.dlt.startup: true}", groupId = "dlt-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.error("ConsumerRecord in DeadLetterTopic Consumer : {}", consumerRecord);
    }

}
