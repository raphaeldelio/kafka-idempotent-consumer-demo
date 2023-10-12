package demo.idempotentconsumer.repository;

import java.util.UUID;

import demo.idempotentconsumer.model.event.outbound.ProcessedInboundEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedInboundEvent, UUID> {
}
