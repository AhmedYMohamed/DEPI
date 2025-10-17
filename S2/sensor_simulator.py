### Generate fake data and send data over kafka
from kafka import KafkaProducer
import time
import random
import json



# Kafka Producer Creation

Producer = KafkaProducer(

    bootstrap_servers = "localhost:9092",
    value_serializer = lambda x  : json.dumps(x).encode("utf-8")
)


for i in range(1000):

    data = {
        "timestamp": time.time(),
        "sensor_id": random.choice(["temp","hum"]),
        "value": random.uniform(20,40)
    }

    if random.random() < 0.05 :
        data["value"] = random.uniform(50,80)

    print(f"Data Sent Sensor#{i+1}")
    Producer.send(topic="S2", value= data)
    time.sleep(1)


Producer.flush()
Producer.close()
