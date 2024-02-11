package com.learnkafka.scheduler;

import com.learnkafka.enums.FailureRecordStatus;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.FailureRecordService;
import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsRetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;

    @Scheduled(fixedRate = 10000L)
    public void retryFailedRecords() {
        log.info("Retrying failed Records Started!");
        failureRecordRepository.findAllByStatus(FailureRecordStatus.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retrying failed Records : {}", failureRecord);
                    ConsumerRecord<Integer, String> consumerRecord = FailureRecordService.failureRecordToConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(FailureRecordStatus.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                    } catch (Exception exception) {
                        log.error("Exception in retryFailedRecords : {}", exception.getMessage(), exception);
                    }
                });
        log.info("Retrying failed Records Completed!");
    }

}
