package demo.idempotentconsumer.repository;

import demo.idempotentconsumer.model.event.outbound.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
}
