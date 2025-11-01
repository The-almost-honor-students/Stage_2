FROM maven:3.8.5-openjdk-17 AS dependencies
LABEL authors="The-almost-honor-students"
WORKDIR /app
COPY pom.xml pom.xml
RUN mvn -B -e org.apache.maven.plugins:maven-dependency-plugin:3.1.2:go-offline -DexcludeArtifactIds=control

FROM maven:3.8.5-openjdk-17 AS builder

WORKDIR /app
COPY --from=dependencies /root/.m2 /root/.m2
COPY --from=dependencies /app/ /app
COPY pom.xml pom.xml
COPY src /app/src
RUN mvn -B -e clean install -DskipTests

FROM openjdk:17-slim

WORKDIR /app
COPY --from=builder /app/target/control-app.jar /app.jar
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app.jar"]
