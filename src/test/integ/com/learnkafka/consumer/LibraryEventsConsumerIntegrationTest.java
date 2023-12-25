package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@EmbeddedKafka(topics = "library-events-test", count = 3, partitions = 3, ports = {8085, 8086, 8087})
@TestPropertySource(properties = {
        "spring.kafka.topic=library-events-test",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LibraryEventsConsumerIntegrationTest {

    @Resource
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Resource
    private KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @BeforeEach
    void setUp() {
        endpointRegistry.getListenerContainers()
                .forEach(messageListenerContainer -> ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @Test
    void testPublishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        // ARRANGE
        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;

        // ACT
        kafkaTemplate.send("library-events-test", json)
                .get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // ASSERT
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

        Assertions.assertThat(libraryEventsRepository.findAll())
                .hasSize(1)
                .first()
                .satisfies(libraryEvent -> {
                   Assertions.assertThat(libraryEvent.getLibraryEventId()).isNotNull();
                   Assertions.assertThat(libraryEvent.getBook().getBookId()).isEqualTo(456);
                   Assertions.assertThat(libraryEvent.getLibraryEventType()).isEqualTo(LibraryEventType.NEW);
                });

    }

}