package com.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point – starts the Spring Boot application.
 *
 * On startup Spring will:
 *  1. Connect to RabbitMQ and declare the queues (RabbitConfig).
 *  2. Connect to the MQTT broker and subscribe to devices/+/data (MqttSubscriber).
 */
@SpringBootApplication
public class MainApplication {

    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }
}
