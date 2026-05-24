# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service does

`storm-repository` is the generic persistence layer of the STORM ETL platform. It has no REST endpoints — all communication is Kafka-only. It listens on topic `REPOSITORY_QUERY`, routes requests to the correct connector executor, and responds on `REPOSITORY_RESPONSE`.

Its role in the platform: storm-api and storm-samdbox publish `REPOSITORY_QUERY` messages; this service executes the actual DB operation and publishes the result.

## Build & Run

```bash
mvn clean package -DskipTests     # build JAR
mvn test                          # run all tests
mvn test -Dtest=ClassName         # run one test class
mvn test -Dtest=ClassName#method  # run one test method
./mvnw spring-boot:run            # run locally (profile: local, port 33000)
```

Local Swagger: `http://localhost:33000/service/doc/swagger-ui/index.html`

The profile is selected by the `AMBIENTE` env var (default: `local`). The `local` profile connects to Kafka at `localhost:29092` with `allow-inline-sensitive-config=true`.

## Architecture

### Message flow

```
REPOSITORY_QUERY (Kafka)
    → KafkaConsumerListener.consumeMessage()
    → MessageHandler.processMessage()
        → resolveRequestTarget()   # inline config or configRef lookup
        → validateRequest()
        → RepositoryConnectorExecutor.execute()
    → KafkaMessageReception.onMessageReceived()   # Observer
REPOSITORY_RESPONSE (Kafka)
```

`MessageHandler` uses the Observer pattern: `KafkaMessageReception` registers as an observer and publishes every `RepositoryMessageDto` it receives to the response topic.

### Two connector extension points

A connector requires **both**:

1. **`ConnectorPlugin`** — metadata only (`id()`, `displayName()`, `version()`, `type()`). Discovered at startup via Java `ServiceLoader`. Must be registered in `src/main/resources/META-INF/services/storm.repository.com.core.api.ConnectorPlugin`.

2. **`RepositoryConnectorExecutor`** — Spring `@Component` bean. Implements `connectorId()` and `execute(operation, config)`. `MessageHandler` builds a `Map<connectorId → executor>` from all beans of this type at construction time.

A `ConnectorPlugin` without a matching `RepositoryConnectorExecutor` bean means the connector is discoverable but returns "Unsupported connectorId" at runtime (current state of `mysql`).

### Connector implementation status

| Connector | Plugin | Executor | Methods |
|-----------|--------|----------|---------|
| `mongo` | ✅ | ✅ | `find`, `insert`, `update`, `delete`, `list_schema` |
| `postgres` | ✅ | ✅ | `list_schema` only — `find`/`insert`/`update`/`delete` not implemented |
| `mysql` | ✅ | ❌ | No executor — all requests return error |

### Kafka request payload (`RepositoryMessageDto`)

The two config modes are mutually exclusive:

- **Inline** (`config` map in payload): only permitted when `app.security.allow-inline-sensitive-config=true`. Blocked by default and must stay off in `dev`/`prod`. Used with `local` profile.
- **configRef** (or `repositoryId`): payload omits `config`; service resolves credentials from `app.repository.targets.<ref>` properties (populated via K8s Secret env vars in `dev`/`prod`).

`databaseName` field in the payload overrides the `database` key in the resolved config — this is how storm-api chat queries target per-project staging databases (`storm_proj_X`).

`storageDriver` is an alias for `connectorId` — both are accepted, with `storageDriver` taking precedence.

### Security

`MessageHandler.containsSensitiveInlineConfig()` checks inline `config` keys against a list of sensitive patterns (`uri`, `password`, `token`, `secret`, `key`, etc.) and rejects the request when `allow-inline-sensitive-config=false`. Error messages are sanitized before publishing (credentials stripped from URIs and `password=` patterns).

### Kafka TLS (dev/prod)

The `dev`/`prod` profiles use `SSL` on port 29092. Truststore is configured via:
- `spring.kafka.properties.ssl.truststore.location` (full path, not filename)
- `spring.kafka.properties.ssl.truststore.password`
- `ssl.endpoint.identification.algorithm` set to `""` (disables hostname verification)

`KafkaConfigValidator` runs at startup and fails fast if Kafka is unreachable or required properties are missing.

## Adding a new connector

1. Create `connectors/<name>/` package.
2. Implement `ConnectorPlugin` (metadata).
3. Add the fully-qualified class name to `META-INF/services/storm.repository.com.core.api.ConnectorPlugin`.
4. Implement `RepositoryConnectorExecutor` as a Spring `@Component` — `connectorId()` must match `ConnectorPlugin.id()`.
5. The `config` map passed to `execute()` contains the resolved target config keys; document what keys your executor requires.

## Key files

| File | Purpose |
|------|---------|
| `core/listener/MessageHandler.java` | Central dispatch: resolves config, validates, routes to executor, publishes response |
| `core/listener/KafkaConsumerListener.java` | `@KafkaListener` entry point |
| `core/adapter/inbound/components/KafkaMessageReception.java` | Observer that publishes to `REPOSITORY_RESPONSE` |
| `core/runtime/ConnectorRegistry.java` | ServiceLoader-based plugin registry |
| `core/config/KafkaConfigValidator.java` | Startup connectivity check (uses raw `AdminClient`, not `KafkaProperties`, due to Spring Boot 4 import split) |
| `core/config/RepositoryTargetRegistry.java` | Resolves `configRef`/`repositoryId` to connector+config |
| `connectors/mongo/MongoConnectorExecutor.java` | Full CRUD + list_schema for MongoDB |
| `connectors/postgres/PostgresConnectorExecutor.java` | `list_schema` only for PostgreSQL |
