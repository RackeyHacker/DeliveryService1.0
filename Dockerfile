FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY common common
COPY supplier-service supplier-service
COPY order-service order-service
COPY delivery-service delivery-service
COPY api-gateway api-gateway
ARG SERVICE
RUN mvn -B -pl ${SERVICE} -am package -DskipTests && cp ${SERVICE}/target/${SERVICE}-1.0.0.jar /app.jar

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app.jar app.jar
USER 10001
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]
