package io.mitochondria.notification.service;

import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.notification.model.ProcessedOrderId;
import io.mitochondria.notification.repository.ProcessedOrderIdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);
    private final ProcessedOrderIdRepository processedOrderIdRepository;

    public NotificationService(ProcessedOrderIdRepository processedOrderIdRepository) {
        this.processedOrderIdRepository = processedOrderIdRepository;
    }

    @KafkaListener(topics = "inventory-reserved")
    public void sendNotificationIfReserved(InventoryReservedEvent inventoryReservedEvent) {
        String orderId = inventoryReservedEvent.getOrderId().toString();
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(
                orderId
            ));
        } catch (DataIntegrityViolationException e) {
            logger.info("Order {} already processed", orderId);
            return;
        }

        logger.info("Received inventory reserved event: {}", inventoryReservedEvent);
    }

    @KafkaListener(topics = "inventory-rejected")
    public void sendNotificationIfRejected(InventoryRejectedEvent inventoryRejectedEvent) {
        String orderId = inventoryRejectedEvent.getOrderId().toString();
        try {
            processedOrderIdRepository.save(new ProcessedOrderId(
                orderId
            ));
        } catch (DataIntegrityViolationException e) {
            logger.info("Order {} already processed", orderId);
            return;
        }

        logger.info("Received inventory rejected event: {}", inventoryRejectedEvent);
    }
}
