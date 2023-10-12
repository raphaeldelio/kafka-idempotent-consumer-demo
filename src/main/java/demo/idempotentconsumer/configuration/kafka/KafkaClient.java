package demo.idempotentconsumer.configuration.kafka;

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaClient {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaClient(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static final String EVENT_ID_HEADER_KEY = "demo_eventIdHeader";
    private static final String OUTBOUND_TOPIC = "demo-outbound-topic";

    public SendResult sendMessage(String key, String data) {
        try {
            String payload = "eventId: " + UUID.randomUUID() + ", payload: " + data;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(OUTBOUND_TOPIC, key, payload);

            final SendResult<String, String> result = kafkaTemplate.send(record).get();
            final RecordMetadata metadata = result.getRecordMetadata();

            log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                    record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

            return result;
        } catch (Exception e) {
            log.error("Error sending message to topic " + OUTBOUND_TOPIC, e);
            throw new KafkaException(e.getMessage());
        }
    }
}