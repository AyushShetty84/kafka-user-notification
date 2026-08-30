# kafka-user-notification

A small event-driven system demonstrating Kafka + Avro + Schema Registry integration between two Spring Boot services: a **user-service** that creates users and publishes events, and a **notification-service** that consumes them.

## Architecture

```
┌──────────────┐   POST /users    ┌──────────────┐
│    Client    │ ───────────────► │ user-service │
└──────────────┘                  │  (port 9050) │
                                   └──────┬───────┘
                                          │ 1. save to PostgreSQL
                                          │ 2. publish UserCreatedEvent (Avro)
                                          ▼
                              ┌────────────────────────┐
                              │   Kafka (broker:9092)   │
                              │  + Schema Registry:8081 │
                              └────────────┬─────────────┘
                                          │ user-created-topic
                                          ▼
                                ┌──────────────────────┐
                                │ notification-service │
                                │     (port 9060)       │
                                └──────────────────────┘
```

Both services share the same Avro schema (`user-created-event.avsc`), compiled into a `UserCreatedEvent` Java class via `avro-maven-plugin`, and serialized/deserialized through Confluent's Kafka Avro (de)serializer against a shared Schema Registry.

## Tech stack

| Layer            | Technology                                      |
|-------------------|--------------------------------------------------|
| Language / Runtime | Java 21                                          |
| Framework          | Spring Boot 4.1 (Web MVC, Data JPA, Kafka)       |
| Messaging          | Apache Kafka (KRaft mode, no ZooKeeper)          |
| Schema management  | Confluent Schema Registry + Apache Avro 1.11     |
| Persistence        | PostgreSQL                                       |
| Build              | Maven                                            |
| Infra (local dev)  | Docker Compose (Confluent Platform 7.7.1)        |

## Services

### `user-service` (port `9050`)

Creates users, persists them to PostgreSQL, and publishes a `UserCreatedEvent` to Kafka on every create.

**Endpoints**

| Method | Path             | Description                                                                 |
|--------|------------------|-------------------------------------------------------------------------------|
| `POST` | `/users`         | Creates a user and publishes a `UserCreatedEvent` to `user-created-topic`.   |
| `POST` | `/users/{message}` | Publishes 1000 test messages (`{message}0` … `{message}999`) to `user-random-topic`, round-robined across keys `0-2`. |

**`POST /users` request body**

```json
{
  "fullName": "abc",
  "email": "abc@gmail.com"
}
```

> `id` is DB-generated (`GenerationType.IDENTITY`) — do not include it in the request body.

### `notification-service` (port `9060`)

Consumes:
- `user-created-topic` → deserializes into `UserCreatedEvent` (Avro).
- `user-random-topic` → plain `String` payloads (three parallel listeners for load-testing/demo purposes).

## Kafka topics

| Topic                | Producer      | Consumer              | Payload                     |
|-----------------------|---------------|------------------------|------------------------------|
| `user-created-topic`  | user-service  | notification-service   | Avro `UserCreatedEvent`     |
| `user-random-topic`   | user-service  | notification-service   | Plain string                |

## Avro schema

`src/main/resources/avro/user-created-event.avsc` (identical copy in both modules):

```json
{
  "name": "UserCreatedEvent",
  "namespace": "com.kafka.event",
  "type": "record",
  "fields": [
    { "name": "id", "type": "long", "default": 0 },
    { "name": "fullName", "type": "string", "default": "" },
    { "name": "email", "type": "string", "default": "" }
  ]
}
```

Code generation is wired via `avro-maven-plugin`, bound to the `generate-sources` phase — running any Maven build (`mvn compile`/`package`) regenerates `com.kafka.event.UserCreatedEvent` under `target/generated-sources/avro`.

## Prerequisites

- **Java 21**
- **Maven** (or use the bundled `./mvnw`)
- **Docker Desktop** (for Kafka, Schema Registry, ksqlDB, etc.)
- **PostgreSQL** running locally on `5432` with a `user_db` database (not included in `docker-compose.yaml` — run it separately, e.g. via a local install or your own Postgres container)

## Getting started

### 1. Start the Kafka stack

```bash
docker compose up -d
```

This brings up:
- `broker` — Kafka (KRaft, port `9092`)
- `schema-registry` — Confluent Schema Registry (port `8081`)
- `connect` — Kafka Connect with datagen connector (port `8083`)
- `control-center` — Confluent Control Center UI (port `9021`)
- `ksqldb-server` / `ksqldb-cli` / `ksql-datagen` — ksqlDB (port `8088`)
- `rest-proxy` — Kafka REST Proxy (port `8082`)

### 2. Set up PostgreSQL

Create a database named `user_db` (both services point at it via `spring.datasource.url`). Update `spring.datasource.username` / `spring.datasource.password` in each service's `application.properties` to match your local setup — **don't commit real credentials**; consider externalizing these via environment variables (`SPRING_DATASOURCE_PASSWORD`, etc.) before deploying anywhere shared.

`spring.jpa.hibernate.ddl-auto=create-drop` is set on both services, so tables are created fresh on every startup — fine for local dev, not for anything persistent.

### 3. Build

```bash
cd user-service && ./mvnw clean package
cd ../notification-service && ./mvnw clean package
```

### 4. Run

```bash
# terminal 1
cd user-service && ./mvnw spring-boot:run

# terminal 2
cd notification-service && ./mvnw spring-boot:run
```

### 5. Try it

```bash
curl -X POST http://localhost:9050/users \
  -H "Content-Type: application/json" \
  -d '{"fullName": "abc", "email": "abc79@gmail.com"}'
```

Watch `notification-service`'s logs for the consumed `UserCreatedEvent`.

## Project structure

```
kafka/
├── docker-compose.yaml        # Local Kafka + Confluent Platform stack
├── user-service/
│   └── src/main/java/com/kafka/user_service/
│       ├── config/            # ModelMapper + Kafka topic bean config
│       ├── controller/        # REST endpoints
│       ├── dto/                # Request DTOs
│       ├── entity/            # JPA entities
│       ├── repository/        # Spring Data repositories
│       └── service/           # Business logic + Kafka producer
└── notification-service/
    └── src/main/java/com/kafka/notification_service/
        └── consumer/           # Kafka listeners
```
