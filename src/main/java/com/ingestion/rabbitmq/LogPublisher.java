package com.ingestion.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * LogPublisher
 * ────────────
 * Sends a structured log / audit event to the RabbitMQ log queue.
 *
 * Log event shape:
 * {
 *   "messageId": "uuid",
 *   "deviceId":  "device-001",
 *   "stage":     "COMPLETED" | "FAILED",
 *   "status":    "SUCCESS"   | "FAILED",
 *   "timestamp": "2024-05-02T10:30:00Z"
 * }
 *
 * A downstream consumer (e.g. a monitoring service) can read this queue
 * to build dashboards, alerts, or audit trails.
 */
@Slf4j
@Component
public class LogPublisher {

    @Value("${rabbitmq.log-queue}")
    private String logQueue;

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public LogPublisher(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Publishes a log event to the log queue.
     *
     * @param messageId  the message being logged
     * @param deviceId   the originating device
     * @param stage      processing stage label (e.g. "COMPLETED", "FAILED")
     * @param status     outcome status        (e.g. "SUCCESS",   "FAILED")
     */
    public void sendLog(String messageId, String deviceId, String stage, String status) {
        try {
            Map<String, String> logEvent = Map.of(
                    "messageId", messageId,
                    "deviceId",  deviceId,
                    "stage",     stage,
                    "status",    status,
                    "timestamp", Instant.now().toString()
            );

            String json = objectMapper.writeValueAsString(logEvent);

            rabbitTemplate.convertAndSend("", logQueue, json);
            log.info("Log event sent to '{}' | messageId={} | stage={} | status={}",
                    logQueue, messageId, stage, status);

        } catch (Exception e) {
            // Log publishing should never crash the main flow
            log.error("Failed to send log event | messageId={} | error={}", messageId, e.getMessage());
        }
    }
}
