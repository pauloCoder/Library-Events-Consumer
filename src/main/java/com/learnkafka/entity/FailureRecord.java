package com.learnkafka.entity;

import com.learnkafka.enums.FailureRecordStatus;
import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class FailureRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Integer id;
    private String topic;
    private Integer cle;
    private String errorRecord;
    private Integer partition;
    private Long offsetValue;
    private String exception;
    @Enumerated(EnumType.STRING)
    private FailureRecordStatus status;
}
