package com.ingestion.mqtt;

import com.ingestion.service.IngestionService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * MqttSubscriber
 * ──────────────
 * Subscribes to devices/+/data on the shared MQTT connection managed by
 * MqttClientManager, and hands each incoming message to IngestionService.
 *
 * Dependency chain (no cycle):
 *   MqttSubscriber → MqttClientManager
 *   MqttSubscriber → IngestionService → AckPublisher → MqttClientManager
 */
@Slf4j
@Component
public class MqttSubscriber {

    @Value("${mqtt.subscribe-topic}")
    private String subscribeTopic;

    private final MqttClientManager mqttClientManager;
    private final IngestionService ingestionService;

    public MqttSubscriber(MqttClientManager mqttClientManager, IngestionService ingestionService) {
        this.mqttClientManager = mqttClientManager;
        this.ingestionService = ingestionService;
    }

    /**
     * Registers the message callback and subscribes to the topic.
     * Runs after MqttClientManager#connect() because Spring initialises
     * beans in dependency order.
     */
    @PostConstruct
    public void subscribe() throws MqttException {
        MqttClient client = mqttClientManager.getClient();

        client.setCallback(new MqttCallback() {

            @Override
            public void connectionLost(Throwable cause) {
                log.error("MQTT connection lost: {}", cause.getMessage());
            }

            /**
             * Called for every incoming MQTT message.
             * topic example: devices/device-001/data
             */
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payload = new String(message.getPayload());
                log.info("MQTT message received | topic={} | payload={}", topic, payload);

                // Extract deviceId: from "devices/{deviceId}/data" use the middle segment,
                // or fall back to the full topic if it has no slashes (e.g. "esp32_pub").
                String[] parts = topic.split("/");
                String deviceId = parts.length >= 2 ? parts[1] : parts[0];

                try {
                    ingestionService.process(deviceId, payload);
                } catch (Exception e) {
                    // Must not let exceptions escape messageArrived –
                    // any uncaught throwable here causes Paho to drop the connection.
                    log.error("Unhandled error processing message | topic={} | error={}", topic, e.getMessage(), e);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not needed for subscriber side
            }
        });

        client.subscribe(subscribeTopic, 1); // QoS 1 – at least once
        log.info("Subscribed to MQTT topic '{}'", subscribeTopic);
    }
}
