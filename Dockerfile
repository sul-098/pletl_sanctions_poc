# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /workspace

# Copy dependency descriptor first for layer caching
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build fat JAR
COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre

# Non-root user for security
RUN groupadd --system appgroup && useradd --system --gid appgroup appuser

WORKDIR /app

COPY --from=build /workspace/target/sanctionspoc-*.jar app.jar

# Sanctions data mount point (Azure File Share PVC in AKS)
VOLUME ["/sanctions-data"]

RUN chown appuser:appgroup /app/app.jar
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
