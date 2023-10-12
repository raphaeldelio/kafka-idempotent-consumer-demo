package demo.idempotentconsumer;

import demo.idempotentconsumer.configuration.kafka.KafkaClient;
import demo.idempotentconsumer.configuration.mapper.JsonMapper;
import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public abstract class KafkaTestBase {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    public void setUp() {
        // Wait until the partitions are assigned.
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    /**
     * Sends a message to the designated [topic] with the given [key] and [event].
     */
    public SendResult sendMessage(String topic, String eventId, String key, InboundEvent event) throws Exception {
        String payload = JsonMapper.writeToJson(event);
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(KafkaClient.EVENT_ID_HEADER_KEY, eventId != null ? eventId.getBytes() : null));
        final ProducerRecord<Long, String> record = new ProducerRecord(topic, null, key, payload, headers);

        final SendResult result = (SendResult)kafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;
    }

    @Configuration
    @Slf4j
    public static class TestConfig {

        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }

        public static class KafkaTestListener {
            public AtomicInteger counter = new AtomicInteger(0);

            @KafkaListener(
                    groupId = "KafkaIdempotentConsumerIntegrationTest",
                    topics = "demo-outbound-topic", autoStartup = "true"
            )
            void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
                log.debug("KafkaTestListener - Received message: " + payload);
                counter.incrementAndGet();
            }
        }
    }
}
