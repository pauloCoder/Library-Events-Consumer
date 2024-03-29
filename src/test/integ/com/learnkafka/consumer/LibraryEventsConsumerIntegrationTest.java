package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@EmbeddedKafka(topics = {"library-events-test", "library-events.RETRY", "library-events.DLT"}, count = 3, partitions = 3, ports = {8085, 8086, 8087})
@TestPropertySource(properties = {
        "spring.kafka.topic=library-events-test",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "topics.retry.startup= false",
        "topics.dlt.startup= false"
})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    private ObjectMapper objectMapper;
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

    @Autowired
    private FailureRecordRepository failureRecordRepository;

    private Consumer<Integer, String> kafkaRecoveryConsumer;

    @Value("${topics.retry.listener}")
    private String retryTopic;

    @Value("${topics.dlt.listener}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {
        endpointRegistry.getListenerContainers()
                .stream()
                .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
                .findFirst()
                .ifPresent(messageListenerContainer -> ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic()));
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

    @Test
    void testPublishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {

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
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(456)
                .bookName("Kafka Using Spring Boot 2.X")
                .bookAuthor("Antonio")
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        // ACT
        kafkaTemplate.send("library-events-test", libraryEvent.getLibraryEventId(), updatedJson)
                .get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // ASSERT

        Assertions.assertThat(libraryEventsRepository.findById(libraryEvent.getLibraryEventId()))
                .isNotNull()
                .get()
                .satisfies(libraryEventVerif -> {
                    Assertions.assertThat(libraryEventVerif.getLibraryEventId()).isNotNull();
                    Assertions.assertThat(libraryEventVerif.getBook().getBookId()).isEqualTo(456);
                    Assertions.assertThat(libraryEventVerif.getLibraryEventType()).isEqualTo(LibraryEventType.UPDATE);
                    Assertions.assertThat(libraryEventVerif.getBook().getBookName()).isEqualTo("Kafka Using Spring Boot 2.X");
                });

    }

    @Test
    void testPublishUpdateLibraryEventWithNullLibraryEventId() throws JsonProcessingException, ExecutionException, InterruptedException {

        // ARRANGE
        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("console-consumer-1901", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Configuration du consumer pour les topics de recovery
        kafkaRecoveryConsumer = new DefaultKafkaConsumerFactory<>(consumerProps, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();

        // ACT
        kafkaTemplate.send("library-events-test", json)
                .get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // ASSERT
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(kafkaRecoveryConsumer, deadLetterTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(kafkaRecoveryConsumer, deadLetterTopic);
        Assertions.assertThat(consumerRecord)
                .isNotNull()
                .satisfies(record -> {
                    Assertions.assertThat(record.topic()).isEqualTo(deadLetterTopic);
                    Assertions.assertThat(json).isEqualTo(record.value());
                });

    }

    @Test
    void testPublishUpdateLibraryEventWithNullLibraryEventIdFailureRecord() throws JsonProcessingException, ExecutionException, InterruptedException {

        // ARRANGE
        String json = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("console-consumer-1901", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Configuration du consumer pour les topics de recovery
        kafkaRecoveryConsumer = new DefaultKafkaConsumerFactory<>(consumerProps, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();

        // ACT
        kafkaTemplate.send("library-events-test", json)
                .get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // ASSERT
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

        long  countDatas = failureRecordRepository.count();
        Assertions.assertThat(countDatas).isEqualTo(1);
        failureRecordRepository.findAll()
                .forEach(failureRecord -> {
                    System.out.println("failureRecord : " + failureRecord);
                });
    }

    @Test
    void testPublishUpdateLibraryEventWithLibraryEventIdNotNullAndExceptionRetry() throws JsonProcessingException, ExecutionException, InterruptedException {

        // ARRANGE
        String json = """
                {
                    "libraryEventId": 999,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("console-consumer-1902", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Configuration du consumer pour les topics de recovery
        kafkaRecoveryConsumer = new DefaultKafkaConsumerFactory<>(consumerProps, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();

        // ACT
        kafkaTemplate.send("library-events-test", json)
                .get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // ASSERT
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(5)).onMessage(Mockito.any(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(5)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(kafkaRecoveryConsumer, retryTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(kafkaRecoveryConsumer, retryTopic);
        Assertions.assertThat(consumerRecord)
                .isNotNull()
                .satisfies(record -> {
                    Assertions.assertThat(record.topic()).isEqualTo(retryTopic);
                    Assertions.assertThat(json).isEqualTo(record.value());
                });

    }

}