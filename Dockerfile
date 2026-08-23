# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml ./
COPY src ./src

RUN mvn -DskipTests package

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app

# Antes del COPY del build: así un cambio de código fuente no invalida esta capa
# ni fuerza volver a descargar el plugin en cada REBUILD_REPOSITORY=1. Versión
# fijada (no `latest`) — mismo criterio que storm-samdbox tras encontrar el mismo
# problema de reproducibilidad en ese Dockerfile (ver CLAUDE.md, incidente paramiko).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://s3.amazonaws.com/session-manager-downloads/plugin/1.2.835.0/ubuntu_64bit/session-manager-plugin.deb" \
        -o /tmp/session-manager-plugin.deb \
    && apt-get install -y --no-install-recommends /tmp/session-manager-plugin.deb \
    && rm /tmp/session-manager-plugin.deb \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && session-manager-plugin --version

COPY --from=build /app/target /app/target
RUN set -e; \
    JAR_FILE=$(ls -1 /app/target/*.jar | grep -v "original" | head -n 1); \
    cp "$JAR_FILE" /app/app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
