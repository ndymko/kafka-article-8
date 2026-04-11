package io.mitochondria.notification.repository;

import io.mitochondria.notification.model.ProcessedOrderId;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedOrderIdRepository extends JpaRepository<ProcessedOrderId, String> {

}