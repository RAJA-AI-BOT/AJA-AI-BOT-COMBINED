FROM maven:3.9-eclipse-temurin-17 AS java-build

WORKDIR /build

COPY pom.xml .
COPY src ./src

RUN mvn -q -DskipTests package


FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY --from=java-build /build/target/dukascopy-bridge-1.0.0.jar /app/bridge.jar

COPY . .

COPY start.sh /app/start.sh

RUN chmod +x /app/start.sh

ENV BRIDGE_PORT=8081
ENV DUKASCOPY_BRIDGE_URL=http://127.0.0.1:8081

EXPOSE 8080

CMD ["/app/start.sh"]
