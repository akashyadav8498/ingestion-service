# ingestion-service

A minimal Spring Boot service that bridges **AWS IoT Core (MQTT)** and **RabbitMQ**.

---

## Flow

```
Device
  │
  │  MQTT publish
  ▼
devices/{deviceId}/data          ← MqttSubscriber listens here
  │
  ▼
IngestionService.process()
  │
  ├─► RabbitMQPublisher  ──────► device.data.queue   (main queue)
  │
  ├─► AckPublisher       ──────► devices/{deviceId}/ack  (MQTT ACK back to device)
  │
  └─► LogPublisher       ──────► device.log.queue    (audit log)
```

---

## Project Structure

```
ingestion-service/
├── pom.xml
└── src/main/
    ├── java/com/ingestion/
    │   ├── MainApplication.java          # Spring Boot entry point
    │   ├── config/
    │   │   └── RabbitConfig.java         # Declares RabbitMQ queues
    │   ├── mqtt/
    │   │   ├── MqttSubscriber.java       # Connects to broker, receives messages
    │   │   └── AckPublisher.java         # Sends ACK back to device via MQTT
    │   ├── rabbitmq/
    │   │   ├── RabbitMQPublisher.java    # Publishes to main queue
    │   │   └── LogPublisher.java         # Publishes to log queue
    │   └── service/
    │       └── IngestionService.java     # Orchestrates the pipeline
    └── resources/
        ├── application.yml
        └── certs/                        # Place your AWS IoT Core TLS certs here
            ├── AmazonRootCA1.pem
            ├── device-cert.pem
            └── device-private.key
```

---

## Class Responsibilities

| Class | Responsibility |
|---|---|
| `MainApplication` | Starts the Spring Boot app |
| `RabbitConfig` | Declares `device.data.queue` and `device.log.queue` as durable queues |
| `MqttSubscriber` | Connects to MQTT broker, subscribes to `devices/+/data`, extracts `deviceId`, calls `IngestionService` |
| `IngestionService` | Orchestrates: publish → ACK → log |
| `RabbitMQPublisher` | Wraps payload in an envelope and sends to the main RabbitMQ queue |
| `AckPublisher` | Builds ACK JSON and publishes to `devices/{deviceId}/ack` via MQTT |
| `LogPublisher` | Builds a log event JSON and sends to the RabbitMQ log queue |

---

## Configuration (`application.yml`)

```yaml
mqtt:
  broker-url: ssl://your-iot-endpoint.amazonaws.com:8883
  client-id: ingestion-service-client
  subscribe-topic: devices/+/data
  ca-cert-path: src/main/resources/certs/AmazonRootCA1.pem
  client-cert-path: src/main/resources/certs/device-cert.pem
  client-key-path: src/main/resources/certs/device-private.key

rabbitmq:
  host: localhost
  port: 5672
  username: guest
  password: guest
  main-queue: device.data.queue
  log-queue: device.log.queue
```

> **Local testing without TLS:** Change `broker-url` to `tcp://localhost:1883` and
> remove the `options.setSocketFactory(...)` line in `MqttSubscriber.java`.

---

## How to Run Locally

### Prerequisites
- Java 17+
- Maven 3.8+
- RabbitMQ running on `localhost:5672` (default guest/guest)
- An MQTT broker (Mosquitto locally, or AWS IoT Core)

### Start RabbitMQ (Docker one-liner, optional)
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### Build & Run
```bash
cd ingestion-service
mvn spring-boot:run
```

---

## Example MQTT Message

**Topic:** `devices/device-001/data`

**Payload:**
```json
{
  "temperature": 23.5,
  "humidity": 60,
  "battery": 87
}
```

### What happens next

1. `MqttSubscriber` receives the message, extracts `deviceId = "device-001"`.
2. `IngestionService` generates `messageId = "550e8400-e29b-41d4-a716-446655440000"`.
3. `RabbitMQPublisher` sends to `device.data.queue`:
   ```json
   {
     "messageId": "550e8400-e29b-41d4-a716-446655440000",
     "deviceId":  "device-001",
     "payload":   "{\"temperature\":23.5,\"humidity\":60,\"battery\":87}"
   }
   ```
4. `AckPublisher` publishes to `devices/device-001/ack`:
   ```json
   { "messageId": "550e8400-...", "status": "SUCCESS" }
   ```
5. `LogPublisher` sends to `device.log.queue`:
   ```json
   {
     "messageId": "550e8400-...",
     "deviceId":  "device-001",
     "stage":     "COMPLETED",
     "status":    "SUCCESS",
     "timestamp": "2024-05-02T10:30:00.123Z"
   }
   ```

---

## Failure Handling

If **any** step throws an exception:
- The error is logged to the console via SLF4J.
- A `FAILED` log event is sent to `device.log.queue`.
- No ACK is sent to the device.

No retry logic is included by design – this is a learning-focused service.
