package io.mitochondria.notification.config;

import io.mitochondria.notification.exception.NonRetryableException;
import io.mitochondria.notification.exception.RetryableException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, e) -> new TopicPartition(
                record.topic() + "-dlt",
                record.partition()
            )
        );
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(3000, 3));

        handler.addNotRetryableExceptions(NonRetryableException.class);
        handler.addRetryableExceptions(RetryableException.class);

        return handler;
    }
}