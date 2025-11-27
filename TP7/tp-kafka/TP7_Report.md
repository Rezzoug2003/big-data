Here is a complete **Markdown (`.md`)** file. You can save this as `TP7_Report.md`. It contains all the code and commands we used, organized step-by-step so you can present it to your teacher.

***

# TP N° 7 : Apache Kafka (Docker & Python)

**Student Name:** [Your Name]
**Date:** 23/11/2025
**Environment:** Windows 10/11, Docker Desktop, Python 3.x

---

## Part 1: Installation (Single Broker)

To start Kafka without installing Java manually, I used **Docker**.

**1. File: `docker-compose.yml` (Single Broker)**
I used the stable version `7.3.2` to avoid compatibility issues.

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.2
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.3.2
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

**2. Start the Environment**
```powershell
docker-compose up -d
docker ps
```

---

## Part 2: Creating a Topic & CLI Testing

**1. Create a Topic**
I entered the container to run Kafka CLI commands.
```powershell
docker exec -it kafka /bin/bash

# Inside the container:
kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**2. Test Producer and Consumer (Console)**
*Terminal 1 (Consumer - Receiver):*
```powershell
docker exec -it kafka /bin/bash
kafka-console-consumer --topic test-topic --bootstrap-server localhost:9092
```

*Terminal 2 (Producer - Sender):*
```powershell
docker exec -it kafka /bin/bash
kafka-console-producer --topic test-topic --bootstrap-server localhost:9092
# Type messages here (e.g., "Hello Kafka")
```

---

## Part 3: IoT Project (Python)

I created a simulation of a machine sensor sending temperature data to a monitoring dashboard.

**Prerequisites:**
```powershell
pip install kafka-python
```

**1. Producer Script (`sensor.py`)**
Simulates a machine sending temperature data every 2 seconds.

```python
import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

machine_id = "Machine-01"
print(f"✅ Sensor started for {machine_id}...")

while True:
    temperature = random.randint(20, 100)
    data = {'machine_id': machine_id, 'temperature': temperature, 'timestamp': time.time()}
    
    producer.send('iot-sensor-data', value=data)
    print(f"Sent: {data}")
    time.sleep(2)
```

**2. Consumer Script (`monitor.py`)**
Reads data and alerts if temperature > 80°C.

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'iot-sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Monitoring System Started...")

for message in consumer:
    data = message.value
    temp = data['temperature']
    
    if temp > 80:
        print(f"⚠️ CRITICAL ALERT! {data['machine_id']} overheating! Temp: {temp}°C")
    else:
        print(f"✅ Normal: {temp}°C")
```

**3. Execution**
```powershell
# Terminal A
python sensor.py

# Terminal B
python monitor.py
```

---

## Part 4: Multiple Brokers Configuration

To test reliability, I configured a cluster with 2 Brokers.

**1. Reset Environment**
```powershell
docker-compose down --volumes --remove-orphans
```

**2. File: `docker-compose.yml` (Cluster)**

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.2
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka1:
    image: confluentinc/cp-kafka:7.3.2
    container_name: kafka1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2

  kafka2:
    image: confluentinc/cp-kafka:7.3.2
    container_name: kafka2
    depends_on:
      - zookeeper
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:29092,PLAINTEXT_HOST://localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
```

**3. Start Cluster**
```powershell
docker-compose up -d
```

**4. Create Replicated Topic**
I created a topic that is copied to both servers (`replication-factor 2`).

```powershell
docker exec -it kafka1 /bin/bash

# Command:
kafka-topics --create --topic critical-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 2

# Verify:
kafka-topics --describe --topic critical-data --bootstrap-server localhost:9092
```
*Result shows `Replicas: 1,2`, confirming the cluster is working.*