package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.enums.FailureRecordStatus;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FailureRecordService {

    private final FailureRecordRepository failureRecordRepository;

    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception exception, FailureRecordStatus status) {
        FailureRecord failureRecord = FailureRecord.builder()
                .topic(consumerRecord.topic())
                .cle(consumerRecord.key())
                .errorRecord(consumerRecord.value())
                .partition(consumerRecord.partition())
                .offsetValue(consumerRecord.offset())
                .exception(exception.getCause().getMessage())
                .status(status)
                .build();
        failureRecordRepository.save(failureRecord);
    }

    public static ConsumerRecord<Integer, String> failureRecordToConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffsetValue(),
                failureRecord.getCle(),
                failureRecord.getErrorRecord()
        );
    }
}
