package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {}", libraryEvent);

        // Simulation d'exception pour vérifier le rejeu d'exception
        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> this.save(libraryEvent);
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is null");
        }
        LibraryEvent libraryEventFromDB = libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                        .orElseThrow(() ->  new IllegalArgumentException("Not a valid Library Event"));
        log.info("Validation is successful for the library Event : {}", libraryEventFromDB);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully persisted the library Event : {}", libraryEvent);
    }

}
