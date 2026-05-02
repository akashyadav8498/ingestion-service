package com.ingestion.config;

import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration.
 *
 * Declares two durable queues so they survive broker restarts:
 *  • main queue  – receives raw device payloads
 *  • log queue   – receives audit / status log events
 *
 * Spring AMQP creates the queues automatically on first connection
 * if they don't already exist.
 */
@Configuration
public class RabbitConfig {

    @Value("${rabbitmq.main-queue}")
    private String mainQueue;

    @Value("${rabbitmq.log-queue}")
    private String logQueue;

    /** Queue that holds the raw device messages. */
    @Bean
    public Queue deviceDataQueue() {
        return new Queue(mainQueue, true); // durable = true
    }

    /** Queue that holds log / audit events. */
    @Bean
    public Queue deviceLogQueue() {
        return new Queue(logQueue, true);
    }
}
