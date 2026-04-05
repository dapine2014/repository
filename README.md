# STORM Repository

Servicio genérico de persistencia para la plataforma STORM. Expone operaciones de datos (find, insert, update, delete) a través de Kafka, con soporte a múltiples tecnologías de almacenamiento mediante plugins (SPI/ServiceLoader).

## Arquitectura

- Monolito modular con carga dinámica de plugins via `ServiceLoader`.
- Ejecutores por conector como beans de Spring (`RepositoryConnectorExecutor`).
- Plugins registrados en `src/main/resources/META-INF/services`.
- Orquestación por Kafka: todas las solicitudes entran por topic y se responden por topic dedicado.

### Carpetas principales

```
src/main/java/storm/repository/com/
├── RepositoryApplication.java
├── core/
│   ├── api/          — Contratos: ConnectorPlugin, ConnectorType, ConnectorConfig
│   ├── runtime/      — ConnectorRegistry (ServiceLoader), RepositoryConnectorExecutor
│   ├── listener/     — KafkaConsumerListener, MessageHandler
│   ├── adapter/      — KafkaMessageObserver, KafkaMessageReception
│   ├── config/       — RepositoryTargetRegistry, RepositorySecurityProperties
│   └── errors/       — Excepciones de dominio
└── connectors/
    └── mongo/        — Plugin + Executor + Client para MongoDB
src/main/resources/META-INF/services/
└── storm.repository.com.core.api.ConnectorPlugin
```

---

## Despliegue (Kubernetes — Minikube)

El servicio corre como `Deployment` en Kubernetes con una imagen Docker construida localmente.

### Manifests (`k8s/`)

```
k8s/
├── repository-secret.yaml      — URI de MongoDB (Secret K8s)
├── repository-deployment.yaml  — Deployment (1 réplica)
└── repository-service.yaml     — Service ClusterIP (puerto 8080)
```

### Build y carga en Minikube

```bash
# 1. Construir imagen
docker build -t storm-repository:latest .

# 2. Cargar en Minikube (método confiable via tar)
docker save storm-repository:latest -o /tmp/storm-repository.tar
minikube cp /tmp/storm-repository.tar /tmp/storm-repository.tar
minikube ssh -- "docker rmi --force storm-repository:latest 2>/dev/null; docker load -i /tmp/storm-repository.tar"

# 3. Aplicar manifests
kubectl apply -f k8s/

# 4. Verificar
kubectl get pods -l app=repository
kubectl rollout status deployment/repository
```

> `minikube image load` no actualiza si el tag ya existe. Usar siempre el método `docker save / docker load`.

### Acceso desde el host

El Service es ClusterIP (interno al cluster). Para acceder desde el host o el navegador:

```bash
kubectl port-forward svc/repository 8080:8080
```

| Endpoint | URL |
|----------|-----|
| Swagger UI | http://localhost:8080/service/doc/swagger-ui/index.html |
| API Docs (OpenAPI) | http://localhost:8080/service/v3/api-docs |
| Health (Actuator) | http://localhost:8080/service/actuator/health |

### Acceso desde otros pods (dentro del cluster)

Usar el DNS interno del Service:

```
http://repository:8080/service/...
```

Ejemplo desde un pod del mismo namespace:
```bash
curl http://repository:8080/service/actuator/health
```

---

## Kafka (request/response)

| Topic | Propiedad | Valor por defecto |
|-------|-----------|-------------------|
| Entrada | `app.kafka.topic.repository-query` | `REPOSITORY_QUERY` |
| Respuesta | `app.kafka.topic.repository-response` | `REPOSITORY_RESPONSE` |

### Modo inline — desarrollo local (`allow-inline-sensitive-config=true`)

Las credenciales van en el payload. Solo válido con perfil `local`.

```json
{
  "type": "REQUEST",
  "requestId": "uuid-123",
  "from": "service-a",
  "to": "repository",
  "connectorId": "mongo",
  "config": {
    "uri": "mongodb://alex:alex8080@localhost:27017/?authSource=admin",
    "database": "storm_data"
  },
  "operation": {
    "method": "find",
    "collection": "users",
    "filter": { "active": true },
    "limit": 100
  }
}
```

### Modo configRef — Kubernetes / producción (recomendado)

Las credenciales se resuelven internamente desde el Secret K8s (`REPOSITORY_TARGET_DEFAULT_URI`). El payload no trae datos sensibles.

```json
{
  "type": "REQUEST",
  "requestId": "uuid-123",
  "from": "service-a",
  "to": "repository",
  "configRef": "default",
  "operation": {
    "method": "find",
    "collection": "users",
    "filter": { "active": true },
    "limit": 100
  }
}
```

### Ejemplos de operaciones (modo configRef)

**Insert:**
```json
{
  "type": "REQUEST",
  "requestId": "req-insert-001",
  "from": "storm-rules",
  "to": "repository",
  "configRef": "default",
  "operation": {
    "method": "insert",
    "collection": "pedidos",
    "payload": {
      "cliente": "Empresa XYZ",
      "total": 1500.00,
      "estado": "pendiente"
    }
  }
}
```

**Update:**
```json
{
  "type": "REQUEST",
  "requestId": "req-update-001",
  "from": "storm-rules",
  "to": "repository",
  "configRef": "default",
  "operation": {
    "method": "update",
    "collection": "pedidos",
    "filter": { "estado": "pendiente" },
    "payload": { "estado": "procesado" }
  }
}
```

**Delete:**
```json
{
  "type": "REQUEST",
  "requestId": "req-delete-001",
  "from": "storm-rules",
  "to": "repository",
  "configRef": "default",
  "operation": {
    "method": "delete",
    "collection": "pedidos",
    "filter": { "_id": "65b9f0e3c2a1b2c3d4e5f678" }
  }
}
```

### Payload response

```json
{
  "type": "RESPONSE",
  "requestId": "uuid-123",
  "correlationId": "uuid-123",
  "from": "repository",
  "to": "service-a",
  "status": "OK",
  "data": {
    "items": [ ],
    "nextCursor": "65b9f0e3c2a1b2c3d4e5f678",
    "totalCount": 1234
  },
  "error": null
}
```

### Enviar mensajes desde el host (Kafka en Docker)

```bash
# find
echo '{"type":"REQUEST","requestId":"test-001","from":"cli","to":"repository","configRef":"default","operation":{"method":"find","collection":"users","filter":{},"limit":10}}' | \
  docker exec -i storm-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic REPOSITORY_QUERY

# Escuchar la respuesta
docker exec storm-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic REPOSITORY_RESPONSE \
  --from-beginning \
  --max-messages 1
```

### Enviar mensajes desde un pod dentro del cluster

```bash
# Abrir shell temporal con acceso a Kafka
kubectl run kafka-client --rm -it --restart=Never \
  --image=confluentinc/cp-kafka:7.4.4 -- bash

# Dentro del pod:
echo '{"type":"REQUEST","requestId":"k8s-test-001","from":"pod-cli","to":"repository","configRef":"default","operation":{"method":"find","collection":"users","filter":{},"limit":5}}' | \
  kafka-console-producer \
    --broker-list 192.168.49.1:29092 \
    --topic REPOSITORY_QUERY

kafka-console-consumer \
  --bootstrap-server 192.168.49.1:29092 \
  --topic REPOSITORY_RESPONSE \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000
```

---

## Paginación (MongoDB)

El `find` soporta cursor por `_id`. El cursor de la siguiente página viene en la respuesta como `nextCursor`.

```json
// Página 1
{ "method": "find", "collection": "pedidos", "filter": {}, "limit": 50 }

// Página 2 — usar nextCursor de la respuesta anterior
{ "method": "find", "collection": "pedidos", "filter": {}, "limit": 50, "cursor": "65b9f0e3c2a1b2c3d4e5f678" }
```

- Sin `limit`: usa `defaultLimit` (200 registros).
- Máximo absoluto: 100.000 registros.

---

## Perfiles de configuración

Controlado con la variable de entorno `AMBIENTE` (default: `local`):

| Perfil | `AMBIENTE` | Puerto | Descripción |
|--------|------------|--------|-------------|
| `local` | `local` (default) | 33000 | Valores hardcoded, inline-config habilitado, sin K8s |
| `dev` | `dev` | 8080 | Variables de entorno, para Minikube / Docker |
| `prod` | `prod` | 8080 | Variables de entorno, credenciales via Secret K8s / Vault |

### Perfil local

```properties
server.port=33000
server.servlet.context-path=/service
spring.kafka.bootstrap-servers=localhost:29092
spring.kafka.consumer.group-id=repository-service
app.security.allow-inline-sensitive-config=true
```

```bash
# Levantar local
./mvnw spring-boot:run
# Swagger: http://localhost:33000/service/doc/swagger-ui/index.html
```

### Perfil dev / prod (variables de entorno)

```bash
AMBIENTE=dev
PORT=8080
SPRING_KAFKA_BOOTSTRAP_SERVERS=192.168.49.1:29092
SPRING_KAFKA_CONSUMER_GROUP_ID=repository-service
REPOSITORY_TARGET_DEFAULT_CONNECTOR=mongo
REPOSITORY_TARGET_DEFAULT_URI=mongodb://user:pass@host:27017/?authSource=admin
REPOSITORY_TARGET_DEFAULT_DATABASE=storm_data
REPOSITORY_ALLOW_INLINE_SENSITIVE_CONFIG=false
```

---

## Seguridad

| Modo | `allow-inline-sensitive-config` | Descripción |
|------|---------------------------------|-------------|
| `local` | `true` | Credenciales permitidas en el payload Kafka |
| `dev` / `prod` | `false` | Solo `configRef` — credenciales en Secret K8s |

> **Nunca** activar `allow-inline-sensitive-config=true` en `dev` ni `prod`.

---

## Plugins

Implementa `ConnectorPlugin` y registra en `META-INF/services`:

```java
public final class MongoConnectorPlugin implements ConnectorPlugin {
    public String id()          { return "mongo"; }
    public String displayName() { return "MongoDB"; }
    public String version()     { return "1.0.0"; }
    public ConnectorType type() { return ConnectorType.SOURCE; }
}
```

Registro en `META-INF/services/storm.repository.com.core.api.ConnectorPlugin`:
```
storm.repository.com.connectors.mongo.MongoConnectorPlugin
```

Para agregar un nuevo conector:
1. Crear clase en `connectors/<nombre>/`.
2. Implementar `ConnectorPlugin` + `RepositoryConnectorExecutor`.
3. Registrar en `META-INF/services`.

---

## Dependencias

| Librería | Versión |
|----------|---------|
| Java | 17 |
| Spring Boot | 4.0.1 |
| Spring Kafka | 3.2.0 |
| MongoDB Driver Sync | (BOM Spring Boot) |
| springdoc-openapi | 2.1.0 |
| Hibernate Validator | 8.0.0 |
