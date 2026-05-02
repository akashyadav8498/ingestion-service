package com.ingestion.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * AckPublisher
 * ────────────
 * Sends an acknowledgement message back to the originating device via MQTT.
 *
 * ACK topic  : devices/{deviceId}/ack
 * ACK payload:
 * {
 *   "messageId": "uuid",
 *   "status":    "SUCCESS"
 * }
 *
 * Depends on MqttClientManager (not MqttSubscriber) to avoid a circular
 * dependency with IngestionService.
 */
@Slf4j
@Component
public class AckPublisher {

    private final MqttClientManager mqttClientManager;
    private final ObjectMapper objectMapper;

    public AckPublisher(MqttClientManager mqttClientManager, ObjectMapper objectMapper) {
        this.mqttClientManager = mqttClientManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Publishes a SUCCESS ACK to the device's ACK topic.
     *
     * @param deviceId   the device to acknowledge
     * @param messageId  the message being acknowledged
     */
    public void sendAck(String deviceId, String messageId) throws Exception {
        String ackTopic = "devices/" + deviceId + "/ack";

        Map<String, String> ackPayload = Map.of(
                "messageId", messageId,
                "status",    "SUCCESS"
        );

        String json = objectMapper.writeValueAsString(ackPayload);

        mqttClientManager.publish(ackTopic, json);
        log.info("ACK sent | topic={} | messageId={}", ackTopic, messageId);
    }
}
