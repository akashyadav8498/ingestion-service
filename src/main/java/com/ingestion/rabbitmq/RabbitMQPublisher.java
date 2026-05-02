package com.ingestion.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * RabbitMQPublisher
 * ─────────────────
 * Sends the raw device message to the RabbitMQ main queue.
 *
 * The message envelope looks like:
 * {
 *   "messageId": "uuid",
 *   "deviceId":  "device-001",
 *   "payload":   { ...original device JSON... }
 * }
 *
 * RabbitTemplate handles the actual AMQP connection and channel management.
 * We use the default exchange ("") with the queue name as the routing key –
 * the simplest possible RabbitMQ setup.
 */
@Slf4j
@Component
public class RabbitMQPublisher {

    @Value("${rabbitmq.main-queue}")
    private String mainQueue;

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public RabbitMQPublisher(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Publishes a device message to the main queue.
     *
     * @param messageId  unique ID generated for this message
     * @param deviceId   device that sent the data
     * @param payload    raw JSON string from the MQTT message
     */
    public void publishToMainQueue(String messageId, String deviceId, String payload) throws Exception {
        // Build the envelope as a simple Map so Jackson serialises it to JSON
        Map<String, Object> envelope = Map.of(
                "messageId", messageId,
                "deviceId",  deviceId,
                "payload",   payload
        );

        String json = objectMapper.writeValueAsString(envelope);

        // convertAndSend(exchange, routingKey, message)
        // Empty exchange = default exchange; routingKey = queue name
        rabbitTemplate.convertAndSend("", mainQueue, json);

        log.info("Published to main queue '{}' | messageId={}", mainQueue, messageId);
    }
}
