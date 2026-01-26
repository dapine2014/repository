# STORM Repository

Repositorio base para conectores en STORM. Implementa una arquitectura monolitica con plugins (SPI/ServiceLoader) para descubrir conectores en tiempo de ejecucion.

## Arquitectura

- Monolito modular con carga dinamica de plugins.
- SPI nativo de Java (`ServiceLoader`).
- Plugins registrados en `src/main/resources/META-INF/services`.
- Orquestacion por Kafka: todas las solicitudes entran por topic y se responden por topic dedicado.

### Carpetas principales

- `src/main/java/storm/repository/com`
  - `RepositoryApplication`: punto de entrada Spring Boot.
- `src/main/java/storm/repository/com/core/api`
  - Contratos base: `ConnectorPlugin`, `ConnectorType`, `ConnectorConfig`.
- `src/main/java/storm/repository/com/core/runtime`
  - `ConnectorRegistry`: carga plugins via `ServiceLoader`.
- `src/main/java/storm/repository/com/core/runtime`
  - `RepositoryConnectorExecutor`: contrato de ejecucion por conector (dinamico).
- `src/main/java/storm/repository/com/core/errors`
  - Errores de dominio (ej. `ConnectorNotFoundException`).
- `src/main/java/storm/repository/com/connectors`
  - Implementaciones de plugins por tecnologia (mysql, mongo, etc.).
- `src/main/resources/META-INF/services`
  - Registro SPI de plugins disponibles en el classpath.

## Kafka (request/response)

- Topic de entrada: `app.kafka.topic.repository-query`
- Topic de respuesta: `app.kafka.topic.repository-response`
- Ejemplos listos: `examples/kafka-request-insert.json`, `examples/kafka-request-find.json`
- Script de prueba: `scripts/kafka-request.sh`

Payload recomendado (request):

```json
{
  "type": "REQUEST",
  "requestId": "uuid-123",
  "from": "service-a",
  "to": "repository",
  "connectorId": "mongo",
  "config": {
    "uri": "mongodb://alex:alex8080@localhost:27017/listenme?authSource=admin",
    "database": "listenme"
  },
  "operation": {
    "method": "find",
    "collection": "users",
    "filter": { "active": true },
    "limit": 100,
    "cursor": null,
    "payload": null
  }
}
```

Payload recomendado (response):

```json
{
  "type": "RESPONSE",
  "requestId": "uuid-123",
  "correlationId": "uuid-123",
  "from": "repository",
  "to": "service-a",
  "status": "OK",
  "data": {
    "items": [ ... ],
    "nextCursor": "65b9f0e3c2a1b2c3d4e5f678",
    "totalCount": 1234
  },
  "error": null
}
```

Ejecutar prueba (insert / find):

```
./scripts/kafka-request.sh examples/kafka-request-insert.json
./scripts/kafka-request.sh examples/kafka-request-find.json
```

## DTOs y flujo

- `RepositoryMessageDto`: sobre unico para request/response.
- `RepositoryOperationDto`: describe la operacion (method, collection, filter, payload) y paginacion (limit, cursor, defaultLimit, maxLimit).
- `MessageHandler` resuelve el `connectorId` y ejecuta via `RepositoryConnectorExecutor`.

## Plugins

Un plugin implementa `ConnectorPlugin` y se registra en `META-INF/services/storm.repository.com.core.api.ConnectorPlugin`.

Ejemplo (MongoDB):

```java
public final class MongoConnectorPlugin implements ConnectorPlugin {
    public String id() { return "mongo"; }
    public String displayName() { return "MongoDB"; }
    public String version() { return "1.0.0"; }
    public ConnectorType type() { return ConnectorType.SOURCE; }
}
```

## MongoDB (driver y conexion)

Se incluyo el driver `mongodb-driver-sync` y un modulo de conexion real:

- `MongoConnectorConfig`: requiere `uri` y `database`.
- `MongoConnectorClient`: crea el cliente y valida conexion con `ping`.
- `MongoConnectorExecutor`: ejecuta operaciones (`find`, `insert`, `update`, `delete`) segun el payload Kafka.

Uso minimo:

```java
Map<String, String> props = Map.of(
    "uri", "mongodb://user:pass@localhost:27017",
    "database", "listenme"
);
ConnectorConfig cfg = new ConnectorConfig(props);
MongoConnectorConfig mongoCfg = new MongoConnectorConfig(cfg);
MongoConnectorClient.validateConnection(mongoCfg);
```

Operaciones (ejemplo):

```json
{
  "operation": {
    "method": "insert",
    "collection": "users",
    "payload": { "name": "Ana", "active": true }
  }
}
```

## Paginacion (Mongo)

El `find` soporta cursor por `_id` y limite de resultados. El cursor se devuelve en la respuesta como `nextCursor`, para que el cliente lo use en la siguiente pagina.

Ejemplo (pagina 1):

```json
{
  "operation": {
    "method": "find",
    "collection": "users",
    "filter": {},
    "limit": 100
  }
}
```

Respuesta:

```json
{
  "data": {
    "items": [ ... ],
    "nextCursor": "65b9f0e3c2a1b2c3d4e5f678",
    "totalCount": 1234
  }
}
```

Ejemplo (pagina 2 con cursor):

```json
{
  "operation": {
    "method": "find",
    "collection": "users",
    "filter": {},
    "limit": 100,
    "cursor": "65b9f0e3c2a1b2c3d4e5f678"
  }
}
```

Limites:

- Si no se envia `limit`, se usa `defaultLimit` (por defecto 200).
- Se puede enviar `maxLimit` para controlar el maximo permitido (por defecto 5000).
- El sistema aplica un maximo absoluto (100000) para evitar consultas excesivas.

## Como agregar un nuevo plugin

1. Crear una clase en `src/main/java/storm/repository/com/connectors/<tu_conector>`.
2. Implementar `ConnectorPlugin`.
3. Registrar la clase en `META-INF/services/storm.repository.com.core.api.ConnectorPlugin`.
4. Opcional: crear un config wrapper y un client de conexion.

## Dependencias

- Java 21
- Spring Boot 4
- MongoDB driver sync

## Desarrollo

```
./mvnw spring-boot:run
```
