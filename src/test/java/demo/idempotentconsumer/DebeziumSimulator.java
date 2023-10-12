package demo.idempotentconsumer;

import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import demo.idempotentconsumer.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;

@Configuration
@Slf4j
public class DebeziumSimulator extends KafkaTestBase {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    private static final String DEMO_OUTBOUND_TOPIC = "demo-outbound-topic";

    /**
     * This method is used to simulate the Debezium connector. It reads all the events from the outbox_event table and
     * sends them to the demo-outbound-topic.
     */
    @Scheduled(fixedDelay = 100)
    public void debeziumSimulatorRunner() {
        outboxEventRepository.findAll().forEach(outboxEvent -> {
            InboundEvent inboundEvent = InboundEvent.builder()
                    .id(outboxEvent.getId().toString())
                    .data(outboxEvent.getPayload())
                    .build();

            try {
                sendMessage(DEMO_OUTBOUND_TOPIC, UUID.randomUUID().toString(), outboxEvent.getId().toString(), inboundEvent);
                outboxEventRepository.delete(outboxEvent);
            } catch (Exception e) {
                log.error("Error sending message to topic {}", DEMO_OUTBOUND_TOPIC, e);
                throw new KafkaException(e.getMessage());
            }
        });
    }
}