package demo.idempotentconsumer.service;

import demo.idempotentconsumer.IntegrationTestBase;
import demo.idempotentconsumer.model.domain.PurchaseOrder;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import demo.idempotentconsumer.repository.PurchaseOrderRepository;
import demo.idempotentconsumer.repository.OutboxEventRepository;
import demo.idempotentconsumer.repository.ProcessedEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test class demonstrates how to achieve idempotence when consuming messages from Kafka.
 *
 * Idempotence is the property of a system to guarantee that multiple executions of a transaction will not produce different results.
 * In this case, we have distributed transactions, where the transaction spans across the Kafka consumer and the database.
 * These tests demonstrate whether certain strategies are idempotent, meaning that the database and the outbound topic are updated only once, even when concurrent transactions begin.
 *
 *     The tests use the following strategies:
 *     - Non-idempotent consumer
 *     - Idempotent consumer
 *     - Idempotent consumer with outbox
 */
@Slf4j
@EmbeddedKafka(partitions = 1, controlledShutdown = true)
class DeduplicationTest extends IntegrationTestBase {

    final static String DEMO_IDEMPOTENT_TEST_TOPIC = "demo-idempotent-inbound-topic";
    final static String DEMO_IDEMPOTENT_OUTBOX_TEST_TOPIC = "demo-idempotent-with-outbox-inbound-topic";
    final static String DEMO_NON_IDEMPOTENT_TEST_TOPIC = "demo-non-idempotent-inbound-topic";

    @Autowired
    private TestConfig.KafkaTestListener testReceiver;

    @Autowired
    private PurchaseOrderRepository purchaseOrderRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private AsyncOrderService asyncOrderService;

    @BeforeEach
    public void setUp() {
        super.setUp();
        testReceiver.counter.set(0);

        // Cleaning database
        purchaseOrderRepository.deleteAll();
        processedEventRepository.deleteAll();
        outboxEventRepository.deleteAll();
    }

    @Test
    public void testNonIdempotent() throws Exception {
        testDeduplication(DEMO_NON_IDEMPOTENT_TEST_TOPIC);
    }

    @Test
    public void testIdempotent() throws Exception {
        testDeduplication(DEMO_IDEMPOTENT_TEST_TOPIC);
    }

    @Test
    public void testIdempotentOutbox() throws Exception {
        testDeduplication(DEMO_IDEMPOTENT_OUTBOX_TEST_TOPIC);
    }

    @Test
    public void testParallelNonIdempotent() throws Exception {
        String key = "someKey";
        InboundEvent inboundEvent = getInboundEvent(key);

        // Send the message multiple times
        repeat(() -> asyncOrderService.processNonIdempotentAsync(key, inboundEvent, false), 3);

        Thread.sleep(10000);
        assertDeduplication();
    }

    @Test
    public void testParallelIdempotent() throws Exception {
        UUID eventId = UUID.randomUUID();
        String key = "someKey";
        InboundEvent inboundEvent = getInboundEvent(key);

        // Send the message multiple times
        repeat(() -> asyncOrderService.processIdempotentAsync(eventId.toString(), key, inboundEvent, false), 3);

        Thread.sleep(10000);
        assertDeduplication();
    }

    @Test
    public void testParallelIdempotentOutbox() throws Exception {
        UUID eventId = UUID.randomUUID();
        String key = "someKey";
        InboundEvent inboundEvent = getInboundEvent(key);

        // Send the message multiple times
        repeat(() -> asyncOrderService.processIdempotentAndOutboxAsync(eventId.toString(), key, inboundEvent, false), 3);

        Thread.sleep(10000);
        assertDeduplication();
    }

    void testDeduplication(String inboundTopic) throws Exception {
        UUID eventId = UUID.randomUUID();
        String key = "someKey";
        InboundEvent inboundEvent = getInboundEvent(key);

        // Send the message multiple times
        sendMessage(inboundTopic, eventId.toString(), key, inboundEvent);
        sendMessage(inboundTopic, eventId.toString(), key, inboundEvent);
        sendMessage(inboundTopic, eventId.toString(), key, inboundEvent);

        Thread.sleep(10000);
        assertDeduplication();
    }

    private void assertDeduplication() {
        assertThat(testReceiver.counter.get(), equalTo(1));
        List<PurchaseOrder> ordersAfterFirstCall = purchaseOrderRepository.findAll();
        assertThat(ordersAfterFirstCall.size(), equalTo(1));
    }

    InboundEvent getInboundEvent(String key) {
        String data = "someData";
        return InboundEvent.builder()
                .id(key)
                .data(data)
                .build();
    }

    void repeat(Runnable action, int times) {
        for (int i = 0; i < times; i++) {
            action.run();
        }
    }
}