package com.ingestion.service;

import com.ingestion.mqtt.AckPublisher;
import com.ingestion.rabbitmq.LogPublisher;
import com.ingestion.rabbitmq.RabbitMQPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * IngestionService
 * ────────────────
 * Orchestrates the full ingestion pipeline for a single device message.
 *
 * Flow:
 *  1. Generate a unique messageId (UUID).
 *  2. Publish the raw payload to RabbitMQ main queue.
 *  3. On success → send ACK back to MQTT  (devices/{deviceId}/ack).
 *  4. On success → send a COMPLETED log event to RabbitMQ log queue.
 *  5. On failure → send a FAILED log event to RabbitMQ log queue.
 *
 * This class is intentionally kept thin – it only coordinates the other
 * components and does not contain any transport-level code itself.
 */
@Slf4j
@Service
public class IngestionService {

    private final RabbitMQPublisher rabbitMQPublisher;
    private final AckPublisher ackPublisher;
    private final LogPublisher logPublisher;

    public IngestionService(RabbitMQPublisher rabbitMQPublisher,
                            AckPublisher ackPublisher,
                            LogPublisher logPublisher) {
        this.rabbitMQPublisher = rabbitMQPublisher;
        this.ackPublisher = ackPublisher;
        this.logPublisher = logPublisher;
    }

    /**
     * Entry point called by MqttSubscriber for every incoming device message.
     *
     * @param deviceId  extracted from the MQTT topic  (devices/{deviceId}/data)
     * @param payload   raw JSON string sent by the device
     */
    public void process(String deviceId, String payload) {
        String messageId = UUID.randomUUID().toString();
        log.info("Processing message | messageId={} | deviceId={}", messageId, deviceId);

        try {
            // ── Step 1: push to RabbitMQ main queue ──────────────────────────
            rabbitMQPublisher.publishToMainQueue(messageId, deviceId, payload);

            // ── Step 2: send ACK back to the device via MQTT ─────────────────
            ackPublisher.sendAck(deviceId, messageId);

            // ── Step 3: log SUCCESS event to RabbitMQ log queue ──────────────
            logPublisher.sendLog(messageId, deviceId, "COMPLETED", "SUCCESS");

            log.info("Message processed successfully | messageId={}", messageId);

        } catch (Exception e) {
            log.error("Failed to process message | messageId={} | error={}", messageId, e.getMessage());

            // ── On failure: log FAILED event to RabbitMQ log queue ───────────
            logPublisher.sendLog(messageId, deviceId, "FAILED", "FAILED");
        }
    }
}
