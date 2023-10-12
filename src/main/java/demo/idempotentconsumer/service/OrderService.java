package demo.idempotentconsumer.service;

import demo.idempotentconsumer.exception.DuplicateEventException;
import demo.idempotentconsumer.configuration.kafka.KafkaClient;
import demo.idempotentconsumer.exception.SimulatedFailureException;
import demo.idempotentconsumer.model.domain.PurchaseOrder;
import demo.idempotentconsumer.model.event.outbound.OutboxEvent;
import demo.idempotentconsumer.model.event.outbound.ProcessedInboundEvent;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import demo.idempotentconsumer.repository.PurchaseOrderRepository;
import demo.idempotentconsumer.repository.OutboxEventRepository;
import demo.idempotentconsumer.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {;

    private KafkaClient kafkaClient;
    private ProcessedEventRepository processedEventRepository;
    private OutboxEventRepository outboxEventRepository;
    private PurchaseOrderRepository purchaseOrderRepository;

    @Autowired
    public OrderService(
            KafkaClient kafkaClient,
            ProcessedEventRepository processedEventRepository,
            OutboxEventRepository outboxEventRepository,
            PurchaseOrderRepository purchaseOrderRepository
    ) {
        this.kafkaClient = kafkaClient;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.purchaseOrderRepository = purchaseOrderRepository;
    }

    @Transactional
    public void processNonIdempotent(
            String key,
            InboundEvent event,
            boolean failDatabase,
            boolean delay
    ) {
        log.debug("Processing non-idempotent event: {}", event);
        if (delay) delay();
        writeOrder(event.getData());
        kafkaClient.sendMessage(key, event.getData());

        if (failDatabase)
            throw new SimulatedFailureException("Database failed to commit");
    }

    @Transactional
    public void processIdempotent(
            String eventId,
            String key,
            InboundEvent event,
            boolean failDatabase,
            boolean delay
    ) {
        log.debug("Processing idempotent event: {}", event);
        deduplicate(UUID.fromString(eventId));

        if (delay) delay();

        writeOrder(event.getData());
        kafkaClient.sendMessage(key, event.getData());

        if (failDatabase)
            throw new SimulatedFailureException("Database failed to commit");
    }

    @Transactional
    public void processIdempotentAndOutbox(
            String eventId,
            InboundEvent event,
            boolean failDatabase,
            boolean delay
    ) {
        log.debug("Processing idempotent+outbox event: {}", event);
        deduplicate(UUID.fromString(eventId));

        if (delay) delay();

        writeOrder(event.getData());
        writeOutboxEvent(event.getData());

        if (failDatabase)
            throw new SimulatedFailureException("Database failed to commit");
    }

    private void deduplicate(UUID eventId) throws DuplicateEventException {
        try {
            processedEventRepository.saveAndFlush(new ProcessedInboundEvent(eventId));
            log.debug("Event persisted with Id: {}", eventId);
        } catch (DataIntegrityViolationException | PessimisticLockingFailureException e) {
            log.warn("Event already processed: {}", eventId);
            throw new DuplicateEventException(eventId);
        }
    }

    private void writeOrder(String payload) {
        PurchaseOrder purchaseOrder = PurchaseOrder.builder()
                .name(payload)
                .timestamp(System.currentTimeMillis())
                .build();
        purchaseOrderRepository.save(purchaseOrder);
    }

    private void writeOutboxEvent(String payload) {
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .version("v1")
                .payload(payload)
                .destination("demo-outbox-outbound")
                .timestamp(System.currentTimeMillis())
                .build();
        UUID outboxEventId = outboxEventRepository.save(outboxEvent).getId();
        log.debug("Event persisted to transactional outbox with Id: {}", outboxEventId);
    }

    private void delay() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
