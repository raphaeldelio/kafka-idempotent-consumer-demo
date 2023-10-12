package demo.idempotentconsumer.repository;

import demo.idempotentconsumer.model.domain.PurchaseOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface PurchaseOrderRepository extends JpaRepository<PurchaseOrder, UUID> {
}
