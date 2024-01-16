import logging
import socket
import time
import uuid
from datetime import datetime
from random import uniform
import random

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from kafka_config.config import read_config, read_source_avro_schema

logging.basicConfig(level=logging.INFO)

def acked(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {msg.value()}: {err}")
    else:
        logging.info(f"Message produced: {msg.value()}")



config = read_config()
source_avro_schema = read_source_avro_schema()
topic = config["kafka"]["subscribe"]

avro_serializer = AvroSerializer(
    schema_registry_client=SchemaRegistryClient(
        {"url": config["kafka"]["schema.registry.url"]}
    ),
    schema_str=source_avro_schema,
)

producer = SerializingProducer(
    {
        "bootstrap.servers": config["kafka"]["kafka.bootstrap.servers"],
        "client.id": socket.gethostname(),
        "value.serializer": avro_serializer,
    }
)

# while True:

for message_count in range (10):

    message = {
        "loan_id": str(uuid.uuid1()),
        "loan_amount": int(uniform(0, 10000000)),
        "borrower_id": str(uuid.uuid1()),
        "status": random.choice(
            ["CLOSED", "SUBMITTED", "LIVE", "CANCELED"]
        ),
        "partner": random.choice(
            ["BANK_A", "BANK_B", "FINTECH_A", "FINTECH_B"]
        ),
        "current_dpd": int(uniform(0, 100)),
        "max_dpd": int(uniform(0, 100)),
        "interest_rate": round(uniform(0, 5), 2),
        "loan_term": int(uniform(0, 100)),
        "created_at": int(datetime.now().timestamp()),
        "updated_at": int(datetime.now().timestamp())
    }

    producer.produce(
        topic=topic,
        value=message,
        on_delivery=acked,
    )

    producer.poll(1)