package demo.idempotentconsumer.consumer;

import demo.idempotentconsumer.configuration.kafka.KafkaClient;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import demo.idempotentconsumer.configuration.mapper.JsonMapper;
import demo.idempotentconsumer.exception.DuplicateEventException;
import demo.idempotentconsumer.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaIdempotentConsumerWithOutbox {

    final AtomicInteger counter = new AtomicInteger();
    final AtomicInteger counterParallel = new AtomicInteger();
    final OrderService orderService;

    @KafkaListener(
            topics = "demo-idempotent-with-outbox-inbound-topic",
            groupId = "kafkaConsumerGroup",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            @Header(KafkaClient.EVENT_ID_HEADER_KEY) String eventId,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Payload final String payload
    ) {
        processMessage(eventId, key, payload, false, false);
    }

    @KafkaListener(
            topics = "demo-atomic-idempotent-with-outbox-inbound-topic",
            groupId = "kafkaConsumerGroup",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenAtomic(
            @Header(KafkaClient.EVENT_ID_HEADER_KEY) String eventId,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Payload final String payload
    ) {
        processMessage(eventId, key, payload, true, false);
    }

    private void processMessage(
            String eventId,
            String key,
            String payload,
            boolean failDatabase,
            boolean delay
    ) {
        counterParallel.getAndIncrement();
        log.debug("Received message [" +counterParallel.get()+ "] - eventId: "+ eventId +" - key: " + key + " - payload: " + payload);
        try {
            InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);
            orderService.processIdempotentAndOutbox(eventId, event, failDatabase, delay);
        } catch (DuplicateEventException e) {
            // Update consumer offsets to ensure event is not again redelivered.
            log.debug("Duplicate message received: "+ e.getMessage());
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
