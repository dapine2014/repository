# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml ./
COPY src ./src

RUN mvn -DskipTests package

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/target /app/target
RUN set -e; \
    JAR_FILE=$(ls -1 /app/target/*.jar | grep -v "original" | head -n 1); \
    cp "$JAR_FILE" /app/app.jar

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" \
        -o /tmp/session-manager-plugin.deb \
    && apt-get install -y --no-install-recommends /tmp/session-manager-plugin.deb \
    && rm /tmp/session-manager-plugin.deb \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && session-manager-plugin --version

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
