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
from utils.spotify_utils import get_user_recently_played_track
from kafka_config.config import read_config, read_spotify_creds, read_source_avro_schema

logging.basicConfig(level=logging.INFO)

def acked(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {msg.value()}: {err}")
    else:
        logging.info(f"Message produced: {msg.value()}")

config = read_config()
source_avro_schema = read_source_avro_schema()
spotify_creds = read_spotify_creds()

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

client_id = spotify_creds["spotify"]["SPOTIFY_CLIENT_ID"]
client_key = spotify_creds["spotify"]["SPOTIFY_CLIENT_SECRET"]
uri = spotify_creds["spotify"]["SPOTIFY_REDIRECT_URI"]
scope = "user-read-recently-played"
cache_filepath = '/root/kafka/kafka_spark_postgresql/kafka_config/token.json'

message = get_user_recently_played_track(client_id, client_key, uri, scope, cache_filepath)

# while True:

for message_count in message:
    
    artist = message_count.get("artist_name")
    title = message_count.get("title")
    album = message_count.get("album_name")
    played_at = message_count.get("played_at")

    payload = {
        "id": str(uuid.uuid1()),
        "artist": str(artist),
        "title": str(title),
        "album": str(album),
        "played_at": str(played_at)
    }

    # print(payload)
    producer.produce(
        topic=topic,
        value=payload,
        on_delivery=acked,
    )


    producer.poll(1)