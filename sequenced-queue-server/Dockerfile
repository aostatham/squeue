FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace
COPY . .
RUN ./mvnw -DskipTests -pl sequenced-queue-server -am package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /workspace/sequenced-queue-server/target/sequenced-queue-server-*-exec.jar /app/sequenced-queue-server.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/sequenced-queue-server.jar"]
