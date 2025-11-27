import time
import json
import random
from kafka import KafkaProducer

# Initialize the Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

machine_id = "Machine-01"

print(f"Starting Sensor for {machine_id}...")

while True:
    # Simulate temperature between 20 and 100
    temperature = random.randint(20, 100)
    
    data = {
        'machine_id': machine_id,
        'temperature': temperature,
        'timestamp': time.time()
    }
    
    # Send data to topic 'iot-sensor-data'
    producer.send('iot-sensor-data', value=data)
    
    print(f"Sent: {data}")
    time.sleep(2) # Send data every 2 seconds