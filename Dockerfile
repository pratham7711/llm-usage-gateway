# One image per service: docker build --build-arg MODULE=gateway -t llmgw/gateway .
# Build the jars first (mvn -DskipTests package) and fetch the agent (make otel-agent).
# --build-arg BASE=eclipse-temurin:21-jdk gives a debug image with jcmd for heap and thread dumps.
ARG BASE=eclipse-temurin:21-jre
FROM ${BASE}
ARG MODULE
WORKDIR /app
RUN useradd --system --uid 10001 app
COPY deploy/otel/opentelemetry-javaagent.jar /otel/opentelemetry-javaagent.jar
COPY ${MODULE}/target/${MODULE}-0.1.0-SNAPSHOT.jar /app/app.jar
USER 10001
# Below 2 CPUs and 1792 MB the JVM is not "server class" and silently picks SerialGC, whose
# stop-the-world pauses reached 447 ms under load here (see docs/DESIGN.md), so the GC is explicit.
ENV JVM_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=65"
ENTRYPOINT ["sh", "-c", "exec java $JVM_OPTS -XX:+ExitOnOutOfMemoryError -jar /app/app.jar"]
