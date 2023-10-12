package demo.idempotentconsumer.service;

import demo.idempotentconsumer.IntegrationTestBase;
import demo.idempotentconsumer.model.domain.PurchaseOrder;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import demo.idempotentconsumer.repository.OutboxEventRepository;
import demo.idempotentconsumer.repository.ProcessedEventRepository;
import demo.idempotentconsumer.repository.PurchaseOrderRepository;
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
 * This test class demonstrates how to achieve atomicity when consuming messages from Kafka.
 *
 *     Atomicity is the property of a system to guarantee that either all operations of a transaction are reflected or none are.
 *     In this case, we have distributed transactions, where the transaction spans across the Kafka consumer and the database.
 *     These tests demonstrate whether certain strategies are atomic, meaning that either both the database and the outbound topic are updated or neither is.
 *
 *     The tests use the following strategies:
 *     - Non-idempotent consumer
 *     - Idempotent consumer
 *     - Idempotent consumer with outbox
 */
@Slf4j
@EmbeddedKafka(partitions = 1, controlledShutdown = true)
class AtomicityTest extends IntegrationTestBase {

    final static String DEMO_ATOMIC_IDEMPOTENT_TEST_TOPIC = "demo-atomic-idempotent-inbound-topic";
    final static String DEMO_ATOMIC_IDEMPOTENT_OUTBOX_TEST_TOPIC = "demo-atomic-idempotent-with-outbox-inbound-topic";
    final static String DEMO_ATOMIC_NON_IDEMPOTENT_TEST_TOPIC = "demo-atomic-non-idempotent-inbound-topic";

    @Autowired
    private TestConfig.KafkaTestListener testReceiver;

    @Autowired
    private PurchaseOrderRepository purchaseOrderRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;


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
        testAtomiticity(DEMO_ATOMIC_NON_IDEMPOTENT_TEST_TOPIC);
    }

    @Test
    public void testIdempotent() throws Exception {
        testAtomiticity(DEMO_ATOMIC_IDEMPOTENT_TEST_TOPIC);
    }

    @Test
    public void testIdempotentOutbox() throws Exception {
        testAtomiticity(DEMO_ATOMIC_IDEMPOTENT_OUTBOX_TEST_TOPIC);
    }

    void testAtomiticity(String inboundTopic) throws Exception {
        UUID eventId = UUID.randomUUID();
        String key = "someKey";
        InboundEvent inboundEvent = getInboundEvent(key);

        sendMessage(inboundTopic, eventId.toString(), key, inboundEvent);

        Thread.sleep(10000);
        assertAtomicity();
    }

    private void assertAtomicity() {
        List<PurchaseOrder> ordersAfterFirstCall = purchaseOrderRepository.findAll();

        log.debug("testReceiver.counter.get() = {}", testReceiver.counter.get());
        log.debug("purchaseOrderRepository.findAll().size() = {}", ordersAfterFirstCall.size());

        assertThat(testReceiver.counter.get(), equalTo(0));
        assertThat(ordersAfterFirstCall.size(), equalTo(0));
    }

    InboundEvent getInboundEvent(String key) {
        String data = "someData";
        return InboundEvent.builder()
                .id(key)
                .data(data)
                .build();
    }
}