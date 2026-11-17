# Kafka Fallback Client (ProducerListener Edition)

A resilient Spring Boot starter for Apache Kafka producers with automatic failover to an HTTP fallback buffer service and built-in circuit breaking, driven by Spring Kafka's **`ProducerListener`**.

Supports both **Spring Boot 2** (Java 8+) and **Spring Boot 3** (Java 17+).

---

## 1. Architecture Overview

```
                               +-----------------------------+
                               |   KafkaFallbackOperations   |
                               |    .publish() / .publishAsync()
                               +--------------+--------------+
                                              |
                                [Fail-Fast Validation: Topic & JSON]
                                              |
                                  +-----------v-----------+
                                  |    Circuit Breaker    |
                                  +-----+-----------+-----+
                                        |           |
                              CLOSED    |           | OPEN / HALF-OPEN
                                        v           v
                              +---------+---+   +---+---------+
                              |    Kafka    |   |  Fallback   |
                              |  Producer   |   | HTTP Client |
                              +----+----+---+   +---+---------+
                                   |    |           ^
                    Broker ACK     |    |           |
                  (onSuccess)      |    | Kafka     | (POST /v1/messages)
                                   |    | Failure   |
                                   |    | (onError) |
                                   v    +-----------+
                      +------------+------------+   |
                      | KafkaFallback           |   |
                      | ProducerListener        |---+
                      +------------+------------+
                                   |
                +------------------+------------------+
                |                                     |
                v                                     v
     +-------------------------+         +-------------------------+
     | PublishResult           |         | PublishResult           |
     | (SENT_TO_KAFKA)         |         | (BUFFERED, messageId)   |
     +-------------------------+         +-------------------------+
```

### Core Mechanisms:
1. **ProducerListener Message Handling**: Replaces individual future chaining with Spring Kafka's native `ProducerListener` (`onSuccess` and `onError`).
   - **`onSuccess`**: Triggered when Kafka broker acknowledges message delivery. Records success in `CircuitBreaker`, logs partition/offset details, and completes the pending `PublishResult` with `SENT_TO_KAFKA`.
   - **`onError`**: Triggered when Kafka dispatch fails (broker error, partition offline, timeout, etc.). Records failure in `CircuitBreaker`, extracts message payload/headers/topic, trips to the HTTP fallback service (`POST /v1/messages`), and completes the future with `BUFFERED(messageId)`.
2. **Dual-Mode Protection**:
   - **High-level API**: Use `KafkaFallbackOperations.publish(...)` or `publishAsync(...)` with fail-fast validation and synchronous/asynchronous results.
   - **Direct `KafkaTemplate` Protection**: Any existing code calling `kafkaTemplate.send(...)` is automatically protected because `KafkaFallbackProducerListener` is registered directly on the `KafkaTemplate`. If Kafka fails, `onError` automatically routes the message to HTTP fallback!
3. **Pre-flight Validation**: Synchronously rejects null/blank topics, null/blank payloads, and invalid JSON strings before any network call.
4. **Resilience & Circuit Breaker**: If Kafka fails repeatedly or the circuit breaker trips `OPEN`, traffic immediately bypasses Kafka and routes directly to the HTTP Fallback service.
5. **Disaster Logging**: If both Kafka and the Fallback HTTP service fail, the event is logged with full payload context at `ERROR` level and a `KafkaFallbackException` is thrown.

---

## 2. Fallback Message Contract (`POST /v1/messages`)

When routing to the fallback service, the client executes an HTTP `POST` request to the configured `kafka.fallback.api-url`:

```http
POST /v1/messages HTTP/1.1
Content-Type: application/json
Authorization: Bearer <token>
token: <token>
X-API-Key: <api-key>

{
  "topic": "order-events",
  "applicationName": "order-service",
  "payload": {
    "orderId": "ORD-1001",
    "status": "CONFIRMED"
  },
  "headers": {
    "traceId": "abc-123"
  }
}
```

### Authentication Headers:
- **`kafka.fallback.token`**: Sends `Authorization: Bearer <token>` and `token: <token>`. (If the configured token already begins with `Bearer `, it is preserved without duplicating).
- **`kafka.fallback.api-key`**: Sends `X-API-Key: <api-key>`.
- Both can be configured together or independently.

### Equivalent cURL:
```bash
curl --location 'https://kafka-fallback.moglilabs.com/v1/messages' \
--header 'Authorization: Bearer sk_3cBtgV5qy8_9NnQQURfU8Pr7NEdbaGb4JLKQSuWbqhA' \
--header 'Content-Type: application/json' \
--data '{
    "topic": "order-events",
    "applicationName": "order-service",
    "payload": {
        "orderId": "ORD-1001",
        "status": "CONFIRMED"
    },
    "headers": {
        "traceId": "abc-123"
    }
}'
```

### Contract Fields:
- `topic` (`String`, required): Target Kafka topic name.
- `applicationName` (`String`, required): Resolved from `kafka.fallback.application-name` (defaults to `spring.application.name`).
- `payload` (`Object` / `JsonNode`, required): The actual JSON payload sent as-is and unmodified.
- `headers` (`Map<String, String>`, optional):
  > When no headers are provided (null or empty), the `headers` field is **completely omitted** from the JSON payload. Internal starter headers (e.g. `x-kafka-fallback-correlation-id`) are automatically stripped before forwarding to the HTTP service.

---

## 3. Dependency Setup

### For Spring Boot 2.x (Java 8, 11, 17)
```xml
<dependency>
    <groupId>com.moglix</groupId>
    <artifactId>kafka-fallback-client-boot2</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### For Spring Boot 3.x (Java 17, 21)
```xml
<dependency>
    <groupId>com.moglix</groupId>
    <artifactId>kafka-fallback-client-boot3</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 4. Quick Start & Usage Options

### Option 1: Via `KafkaFallbackOperations` (Recommended)

Inject `KafkaFallbackOperations` into your service:

```java
import com.moglix.kafka.fallback.client.KafkaFallbackOperations;
import com.moglix.kafka.fallback.model.PublishResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventService {

    @Autowired
    private KafkaFallbackOperations kafkaFallback;

    // 1. Synchronous publish with JSON String
    public void sendOrderJson(String orderJson) {
        PublishResult result = kafkaFallback.publish("order-events", orderJson);
        System.out.println("Status: " + result.getStatus()); // SENT_TO_KAFKA or BUFFERED
    }

    // 2. Synchronous publish with Java Object and custom headers
    public void sendOrder(OrderDto orderDto, String traceId) {
        Map<String, String> headers = Collections.singletonMap("traceId", traceId);
        PublishResult result = kafkaFallback.publish("order-events", orderDto, headers);
        
        if (result.isBuffered()) {
            System.out.println("Message buffered with ID: " + result.getMessageId());
        }
    }

    // 3. Asynchronous publish
    public CompletableFuture<PublishResult> sendOrderAsync(OrderDto orderDto) {
        return kafkaFallback.publishAsync("order-events", orderDto);
    }
}
```

### Option 2: Direct `KafkaTemplate` with `ProducerListener`

If your codebase already uses `KafkaTemplate<String, Object>`:
`KafkaFallbackProducerListener` is automatically registered on `KafkaTemplate`. If Kafka fails, `ProducerListener.onError(...)` will catch the failure and route the message to the HTTP fallback service automatically!

```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void sendDirect(String topic, String jsonPayload) {
    // If Kafka delivery fails, KafkaFallbackProducerListener automatically routes to HTTP fallback
    kafkaTemplate.send(topic, jsonPayload);
}
```

---

## 5. Configuration Reference

All properties are configured under `kafka.fallback.*`:

| Property | Default | Description |
| :--- | :--- | :--- |
| `kafka.fallback.enabled` | `true` | Enable or disable the fallback starter |
| `kafka.fallback.application-name` | `${spring.application.name}` | Calling application name sent in fallback DTO |
| `kafka.fallback.api-url` | *(Required)* | HTTP fallback endpoint (e.g. `https://kafka-fallback.moglilabs.com/v1/messages`) |
| `kafka.fallback.token` | `""` | Optional Auth Token sent in `Authorization: Bearer <token>` and `token` headers |
| `kafka.fallback.api-key` | `""` | Optional API Key sent in `X-API-Key` HTTP header |
| `kafka.fallback.timeouts.kafka-send-timeout-ms` | `2000` | Max timeout for Kafka delivery before fallback |
| `kafka.fallback.timeouts.api-connect-timeout-ms` | `1000` | HTTP connect timeout for fallback service |
| `kafka.fallback.timeouts.api-read-timeout-ms` | `3000` | HTTP read timeout for fallback service |
| `kafka.fallback.circuit-breaker.failure-threshold` | `5` | Consecutive Kafka failures before tripping breaker OPEN |
| `kafka.fallback.circuit-breaker.wait-duration-in-open-ms` | `10000` | Time in ms before testing Kafka with a HALF_OPEN probe |
| `kafka.fallback.retry.max-attempts` | `2` | Number of retry attempts on HTTP fallback call |
| `kafka.fallback.retry.backoff-ms` | `200` | Backoff multiplier in ms between HTTP retries |

### Recommended Kafka Producer Configuration (Fast Failover Tuning)

> [!IMPORTANT]
> **Why this is critical:**
> By default, Apache Kafka's client sets `delivery.timeout.ms` to `120000` ms (2 minutes) and `max.block.ms` to `60000` ms (1 minute). If your Kafka cluster goes down, the producer would block and wait for up to **120 seconds** before reporting an error, delaying the HTTP fallback!
> 
> To enable **near-instantaneous failover** to the HTTP fallback service (within 2–3 seconds), configure low producer timeouts as shown below.

### Example `application.yml`:
```yaml
spring:
  application:
    name: order-service
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: 1
      retries: 3
      properties:
        delivery.timeout.ms: 3000   # Max time for delivery before triggering onError
        request.timeout.ms: 2000    # Broker socket request timeout
        max.block.ms: 2000          # Max block time for metadata fetch when broker is down

kafka:
  fallback:
    enabled: true
    application-name: order-service
    api-url: https://kafka-fallback.moglilabs.com/v1/messages
    token: ${FALLBACK_AUTH_TOKEN:sk_3cBtgV5qy8_9NnQQURfU8Pr7NEdbaGb4JLKQSuWbqhA}
    # api-key: secret-api-key # optional alternative for gateways using X-API-Key
    timeouts:
      kafka-send-timeout-ms: 3000
      api-connect-timeout-ms: 1000
      api-read-timeout-ms: 2500
    circuit-breaker:
      failure-threshold: 3
      wait-duration-in-open-ms: 8000
    retry:
      max-attempts: 2
      backoff-ms: 200
```

### Example `application.properties`:
```properties
spring.application.name=order-service
spring.kafka.bootstrap-servers=localhost:9092

# Fast Failover Producer Settings (Crucial for immediate fallback)
spring.kafka.producer.acks=1
spring.kafka.producer.retries=3
spring.kafka.producer.properties.delivery.timeout.ms=3000
spring.kafka.producer.properties.request.timeout.ms=2000
spring.kafka.producer.properties.max.block.ms=2000

# Fallback Starter Configuration
kafka.fallback.enabled=true
kafka.fallback.application-name=order-service
kafka.fallback.api-url=https://kafka-fallback.moglilabs.com/v1/messages
kafka.fallback.token=${FALLBACK_AUTH_TOKEN:sk_3cBtgV5qy8_9NnQQURfU8Pr7NEdbaGb4JLKQSuWbqhA}
# kafka.fallback.api-key=secret-api-key
kafka.fallback.timeouts.kafka-send-timeout-ms=3000
kafka.fallback.timeouts.api-connect-timeout-ms=1000
kafka.fallback.timeouts.api-read-timeout-ms=2500
kafka.fallback.circuit-breaker.failure-threshold=3
kafka.fallback.circuit-breaker.wait-duration-in-open-ms=8000
kafka.fallback.retry.max-attempts=2
kafka.fallback.retry.backoff-ms=200
```

---

## 6. Build and Verification

### Prerequisites
- **JDK 17 or 21**: Required to build the full multi-module project (the `kafka-fallback-client-boot3` module targets Spring Boot 3 / Java 17).
- **JDK 8+**: Supported if compiling only the Boot 2 stack (`kafka-fallback-client-core` and `kafka-fallback-client-boot2`).

### Commands

Build and run tests across all modules (using JDK 17/21):
```bash
./mvnw clean test
```

Build only the Spring Boot 2 starter on Java 8:
```bash
./mvnw clean install -pl :kafka-fallback-client-core,:kafka-fallback-client-boot2
```

Verify Boot 2 and Boot 3 structural and API parity:
```bash
python3 scripts/check-module-sync.py
```
