import psycopg2

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from kafka_config.config import read_config, read_source_avro_schema

config = read_config()
source_avro_schema = read_source_avro_schema()

topic = config["kafka"]["subscribe"]

def conn_postgres(HOST, USERNAME, PASSWORD, DATABASE, PORT):
    conn = psycopg2.connect(host = HOST, 
                            user = USERNAME, 
                            password = PASSWORD, 
                            database = DATABASE, 
                            port= PORT)
    cur = conn.cursor()
    return conn, cur

HOST = config["postgresql"]["host"]
USERNAME = config["postgresql"]["user"]
PASSWORD = config["postgresql"]["password"]
DATABASE = config["postgresql"]["database"]
PORT = config["postgresql"]["port"]

avro_deserializer = AvroDeserializer(
    schema_registry_client=SchemaRegistryClient(
        {"url": config["kafka"]["schema.registry.url"]}
    ),
    schema_str=source_avro_schema,
)

consumer_conf = {
        "bootstrap.servers": config["kafka"]["kafka.bootstrap.servers"],
        "group.id": config["kafka"]["group.id"],
        "auto.offset.reset": config["kafka"]["auto.offset.reset"]
    }

consumer = DeserializingConsumer(consumer_conf)

consumer.subscribe([topic])

connection, cursor = conn_postgres(HOST, USERNAME, PASSWORD, DATABASE, PORT)

try:
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS public.spotify_user_recently_played_song (
        id varchar(255),
        artist varchar(255),
        title varchar(255),
        album varchar(255),
        played_at varchar(255)
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    
    while True:
        msg = consumer.poll(1)
        
        if msg is None:
            continue
        else:
            data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            print(data)
            
            id = str(data["id"])
            artist = str(data["artist"])
            title = str(data["title"])
            album = str(data["album"])
            played_at = str(data["played_at"])
            
            insert_query = f"""INSERT INTO public.spotify_user_recently_played_song (id,artist,title,album,played_at) VALUES (%s,%s,%s,%s,%s)"""
            
           
            
            # print(insert_query)
            cursor.execute(insert_query, (id, artist, title, album, played_at))
            connection.commit()
            
except Exception as err:
    print(f"Error: {err}")
    

consumer.close()
cursor.close()
connection.close()