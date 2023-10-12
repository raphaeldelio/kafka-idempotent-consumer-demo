package demo.idempotentconsumer.consumer;


import demo.idempotentconsumer.configuration.mapper.JsonMapper;
import demo.idempotentconsumer.exception.DuplicateEventException;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
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
public class KafkaNonIdempotentConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final OrderService orderService;

    @KafkaListener(
            topics = "demo-non-idempotent-inbound-topic",
            groupId = "kafkaConsumerGroup",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload final String payload) {
        processMessage(key, payload, false, false);
    }

    @KafkaListener(
            topics = "demo-atomic-non-idempotent-inbound-topic",
            groupId = "kafkaConsumerGroup",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1"
    )
    public void listenAtomic(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload final String payload) {
        processMessage(key, payload, true, false);
    }

    private void processMessage(String key, String payload, boolean failDatabase, boolean delay) {
        counter.getAndIncrement();
        log.debug("Received message [" +counter.get()+ "] - key: " + key + " - payload: " + payload);
        try {
            InboundEvent event = JsonMapper.readFromJson(payload, InboundEvent.class);
            orderService.processNonIdempotent(key, event, failDatabase, delay);
        } catch (DuplicateEventException e) {
            // Update consumer offsets to ensure event is not again redelivered.
            log.debug("Duplicate message received: "+ e.getMessage());
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
