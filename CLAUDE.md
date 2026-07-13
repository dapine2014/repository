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

The profile is selected by the `AMBIENTE` env var (default: `local`). The `local` profile connects to Kafka at `localhost:29092` with `app.security.allow-inline-sensitive-config=true`. Only `local`, `dev`, and `prod` profiles exist (no `qa` profile in this service).

Unlike the other STORM Java services, this module has real unit tests (not just a Spring-context smoke test) — see `src/test/java/storm/repository/com/core/listener/MessageHandlerTest.java` for the security/configRef/observer behavior and `ConnectorRegistryTest.java` for ServiceLoader discovery. Sample Kafka request payloads for manual testing live in `examples/*.json`.

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

A `ConnectorPlugin` without a matching `RepositoryConnectorExecutor` bean means the connector is discoverable but returns "Unsupported connectorId" at runtime.

### Connector implementation status

All seven connectors below are **fully implemented** (both `ConnectorPlugin` and `RepositoryConnectorExecutor`), each with `list_schema`, `find`, `insert`, `update`, `delete`; all SQL connectors also implement `save` (upsert).

| Connector | JDBC driver / client | Identifier quoting | Upsert (`save`) strategy | Schema source |
|-----------|----------------------|---------------------|---------------------------|---------------|
| `mongo` | MongoDB driver | n/a (no `save`) | — | n/a |
| `postgres` | `postgresql` | double quotes | `ON CONFLICT (...) DO UPDATE SET` | `information_schema.columns` (public schema) |
| `mysql` | `mysql-connector-j` | backticks | `ON DUPLICATE KEY UPDATE` | `information_schema.COLUMNS` (scoped to database) |
| `oracle` | `ojdbc11` (thin mode, no Instant Client) | double quotes | `MERGE INTO ... USING DUAL` | `USER_TABLES`/`USER_TAB_COLUMNS` |
| `redis` | `jedis` | n/a (Hashes) | native `save` semantics | key-prefix SCAN sampling |
| `sqlserver` | `mssql-jdbc` | `[brackets]` | `MERGE INTO ... WITH (HOLDLOCK)` | `INFORMATION_SCHEMA.COLUMNS` (schema `dbo`) |
| `db2` | `jcc` (IBM DB2) | double quotes, uppercased identifiers | `MERGE INTO ... USING SYSIBM.SYSDUMMY1` | `SYSCAT.COLUMNS`/`SYSCAT.TABLES` |

Notes on the SQL executors (postgres/mysql/oracle/sqlserver/db2 all follow the same shape — `find`/`insert`/`update`/`delete`/`save`/`listSchema` — differing only in dialect quirks):
- Pagination limits are resolved uniformly via `resolveEffectiveLimit()`: `operation.limit` (falling back to `operation.defaultLimit`, default 200) capped at `operation.maxLimit` (default 5000), with an absolute hard cap of 100,000 regardless of what the caller requests.
- `delete` always requires a non-empty `filter` — full-table deletes are rejected.
- `save` picks the PK column as `id`, then `_id`, then the first key in the payload map if neither is present.
- Error messages are sanitized (`sanitize()`) to strip `user:pass@` credentials before being included in exceptions.
- `oracle` additionally auto-detects SID vs Service Name connection strings (`host:port:SID` vs `host:port/service`) from the `database` config key.

`dynamodb/`, `firebase/`, `firestore/`, `mssql/` (legacy placeholder name — superseded by `sqlserver/`), `neo4j/`, `s3/` — **not present / empty placeholder directories**, no Java files.

This means schema discovery (`GET /projects/{id}/schema` on storm-api) and ETL LOAD via storm-repository work end-to-end for: postgres, mysql, oracle, sqlserver, db2, redis. Note: the ETL sandbox (`storm-samdbox`) connects to source/destination databases directly via `driver_factory.py`, not through storm-repository — storm-repository's connector coverage governs schema-discovery and chat-query paths, not the sandbox's own DB support.

### Kafka request payload (`RepositoryMessageDto`)

The two config modes are mutually exclusive:

- **Inline** (`config` map in payload): only permitted when `app.security.allow-inline-sensitive-config=true`. Blocked by default and must stay off in `dev`/`prod`. Used with `local` profile.
- **configRef** (or `repositoryId`): payload omits `config`; service resolves credentials from `app.repository.targets.<ref>` properties (populated via K8s Secret env vars in `dev`/`prod`).

`databaseName` field in the payload overrides the `database` key in the resolved config — this is how storm-api chat queries target per-project staging databases (`storm_proj_X`).

`storageDriver` is an alias for `connectorId` — both are accepted, with `storageDriver` taking precedence.

`RepositoryOperationDto` fields: `method`, `collection`, `filter` (Map), `payload` (Object — single doc or list for batch insert), `limit`, `cursor`, `defaultLimit`, `maxLimit`.

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
6. Add the JDBC driver / client dependency to `pom.xml` if it's a new database family.

The five relational connectors (postgres/mysql/oracle/sqlserver/db2) are near-identical in structure — copying the closest existing one (e.g. `oracle` for another relational DB, `redis` for another KV/document store) and adjusting dialect-specific SQL (quoting, upsert syntax, schema catalog tables, pagination clause) is the fastest path to a new connector.

## Key files

| File | Purpose |
|------|---------|
| `core/listener/MessageHandler.java` | Central dispatch: resolves config, validates, routes to executor, publishes response |
| `core/listener/KafkaConsumerListener.java` | `@KafkaListener` entry point |
| `core/adapter/inbound/components/KafkaMessageReception.java` | Observer that publishes to `REPOSITORY_RESPONSE` |
| `core/runtime/ConnectorRegistry.java` | ServiceLoader-based plugin registry |
| `core/runtime/RepositoryConnectorExecutor.java` | Executor interface: `connectorId()` + `execute(operation, config)` |
| `core/dto/RepositoryOperationDto.java` | `method`/`collection`/`filter`/`payload`/`limit`/`cursor`/`defaultLimit`/`maxLimit` |
| `core/config/KafkaConfigValidator.java` | Startup connectivity check (uses raw `AdminClient`, not `KafkaProperties`, due to Spring Boot 4 import split) |
| `core/config/RepositoryTargetRegistry.java` | Resolves `configRef`/`repositoryId` to connector+config |
| `connectors/mongo/MongoConnectorExecutor.java` | Full CRUD + list_schema for MongoDB |
| `connectors/postgres/PostgresConnectorExecutor.java` | Full CRUD + list_schema for PostgreSQL; `save` uses `ON CONFLICT (...) DO UPDATE SET`; identifiers double-quoted |
| `connectors/mysql/MysqlConnectorExecutor.java` | Full CRUD + list_schema for MySQL; `save` uses `ON DUPLICATE KEY UPDATE`; identifiers backtick-quoted; JDBC URL includes `useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC` |
| `connectors/oracle/OracleConnectorExecutor.java` | Full CRUD + list_schema for Oracle; `save` uses `MERGE INTO...USING DUAL`; pagination via `FETCH FIRST n ROWS ONLY`; `list_schema` queries `USER_TABLES`/`USER_TAB_COLUMNS`; supports SID and Service Name |
| `connectors/redis/RedisConnectorExecutor.java` | Full CRUD + list_schema for Redis; documents as Hashes under `{collection}:{id}` keys; `list_schema` discovers key prefixes via SCAN; uses Jedis |
| `connectors/sqlserver/SqlServerConnectorExecutor.java` | Full CRUD + list_schema for SQL Server; `save` uses `MERGE INTO ... WITH (HOLDLOCK)`; pagination via `SELECT TOP n`; `list_schema` queries `INFORMATION_SCHEMA.COLUMNS` scoped to `dbo`; identifiers `[bracket]`-quoted |
| `connectors/db2/Db2ConnectorExecutor.java` | Full CRUD + list_schema for DB2; `save` uses `MERGE INTO ... USING SYSIBM.SYSDUMMY1`; pagination via `FETCH FIRST n ROWS ONLY`; `list_schema` queries `SYSCAT.COLUMNS`/`SYSCAT.TABLES` scoped to the connecting user's schema; identifiers double-quoted and uppercased |
