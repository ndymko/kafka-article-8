package io.mitochondria.inventory.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mitochondria.inventory.dto.InventoryRejectedDto;
import io.mitochondria.inventory.dto.InventoryReservedDto;
import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.inventory.model.OutboxEvent;
import io.mitochondria.inventory.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InventoryRelayerScheduler {
    private final Logger logger = LoggerFactory.getLogger(InventoryRelayerScheduler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryRelayerScheduler(
        OutboxEventRepository outboxEventRepository,
        KafkaTemplate<String, Object> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 5000)
    public void relayOutboxEvents() {
        List<OutboxEvent> outboxEvents = outboxEventRepository.findBySentFalseOrderByCreatedAtAsc(PageRequest.of(0, 100));

        for (OutboxEvent outboxEvent : outboxEvents) {
            try {
                String topic = outboxEvent.getTopic();
                String payloadJson = outboxEvent.getPayload();
                Object payload = null;

                switch (topic) {
                    case "inventory-reserved" -> {
                        InventoryReservedDto inventoryReservedDto = objectMapper.readValue(payloadJson, InventoryReservedDto.class);
                        payload = new InventoryReservedEvent(inventoryReservedDto.orderId(), inventoryReservedDto.email());
                    }
                    case "inventory-rejected" -> {
                        InventoryRejectedDto inventoryRejectedDto = objectMapper.readValue(payloadJson, InventoryRejectedDto.class);
                        payload = new InventoryRejectedEvent(inventoryRejectedDto.orderId(), inventoryRejectedDto.email());
                    }
                }

                kafkaTemplate.send(outboxEvent.getTopic(), outboxEvent.getKey(), payload).get();
                outboxEvent.setSent(true);
                outboxEventRepository.save(outboxEvent);
            }
            catch (Exception e) {
                logger.error("Failed to relay outbox event: {}", outboxEvent.getKey(), e);
            }
        }
    }
}
