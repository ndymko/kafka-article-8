package io.mitochondria.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mitochondria.inventory.dto.InventoryRejectedDto;
import io.mitochondria.inventory.dto.InventoryReservedDto;
import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.inventory.model.OutboxEvent;
import io.mitochondria.inventory.model.ProcessedOrderId;
import io.mitochondria.inventory.repository.InventoryRepository;
import io.mitochondria.inventory.repository.OutboxEventRepository;
import io.mitochondria.inventory.repository.ProcessedOrderIdRepository;
import io.mitochondria.order.event.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class InventoryService {
    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);
    private final InventoryRepository inventoryRepository;
    private final ProcessedOrderIdRepository processedOrderIdRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;

    public InventoryService(
        InventoryRepository inventoryRepository,
        ProcessedOrderIdRepository processedOrderIdRepository,
        OutboxEventRepository outboxEventRepository,
        TransactionTemplate transactionTemplate,
        ObjectMapper objectMapper
    ) {
        this.inventoryRepository = inventoryRepository;
        this.processedOrderIdRepository = processedOrderIdRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order-placed")
    public void reserveInventory(OrderPlacedEvent orderPlacedEvent) {
        transactionTemplate.executeWithoutResult(status -> {
            processOrderInTransaction(orderPlacedEvent);
        });
    }

    private void processOrderInTransaction(OrderPlacedEvent orderPlacedEvent) {
        String orderId = orderPlacedEvent.getOrderId().toString();
        String email = orderPlacedEvent.getEmail().toString();
        String productName = orderPlacedEvent.getProductName().toString();
        int quantity = orderPlacedEvent.getQuantity();

        try {
            processedOrderIdRepository.save(new ProcessedOrderId(
                orderId
            ));
        } catch (DataIntegrityViolationException e) {
            logger.info("Order {} already processed", orderId);
            return;
        }

        int count = inventoryRepository.deductStock(productName, quantity);
        String topic = (count > 0) ? "inventory-reserved" : "inventory-rejected";
        Object dto = (count > 0)
            ? new InventoryReservedDto(orderId, email)
            : new InventoryRejectedDto(orderId, email);
        String json;

        try {
            json = objectMapper.writeValueAsString(dto);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for order: " + orderId, e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
            orderId,
            topic,
            json
        );

        outboxEventRepository.save(outboxEvent);
    }
}
