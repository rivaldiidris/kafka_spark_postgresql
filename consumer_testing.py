import csv
import os
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from kafka_config.config import read_config, read_source_avro_schema

config = read_config()
source_avro_schema = read_source_avro_schema()
topic = config["kafka"]["subscribe"]

class Events(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color
    """

    def __init__(
            self, 
            message_count=None,
            event_id=None, 
            user_id=None, 
            movie_id=None,
            rating=None,
            rating_timestamp=None
        ):
        self.message_count = int(message_count)
        self.event_id = str(event_id)
        self.user_id = str(user_id)
        self.movie_id = str(movie_id)
        self.rating = float(rating)
        self.rating_timestamp = int(rating_timestamp)

def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return Events(
        message_count=obj['message_count'],
        event_id=obj['event_id'],
        user_id=obj['user_id'],
        movie_id=obj['movie_id'],
        rating=obj['rating'],
        rating_timestamp=obj['rating_timestamp']
    )

avro_deserializer = AvroDeserializer(
    SchemaRegistryClient(
        {"url": config["kafka"]["schema.registry.url"]}
    ),
    source_avro_schema,
    dict_to_user
)

consumer = Consumer(
    {
        "bootstrap.servers": config["kafka"]["kafka.bootstrap.servers"],
        "group.id": config["kafka"]["group.id"],
        "auto.offset.reset": config["kafka"]["auto.offset.reset"]
    }
)
consumer.subscribe([topic])

## Events = 1 Files Result
# csv_file_path = "results.csv"
# with open(csv_file_path, mode='w', newline='') as csv_file:
#     # Create a CSV writer
#     csv_writer = csv.writer(csv_file)

#     # Write the header row to the CSV file
#     csv_writer.writerow(["message_count", "event_id", "user_id", "movie_id", "rating", "rating_timestamp"])

#     while True:
#         try:
            
#             msg = consumer.poll(1)
#             if msg is None:
#                 continue

#             Message = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
#             if Message is not None:
#                 print("Message record {}: message_count : {}\n"
#                         "\ event_id: {}\n"
#                         "\ user_id: {}\n"
#                         "\ movie_id: {}\n"
#                         "\ rating: {}\n"
#                         "\ rating_timestamp: {}\n"
#                         .format(
#                             msg.key(),
#                             Message.message_count,
#                             Message.event_id,
#                             Message.user_id,
#                             Message.movie_id,
#                             Message.rating,
#                             Message.rating_timestamp
#                         )
#                 )

#                 # Write the data to the CSV file
#                 csv_writer.writerow([
#                     Message.message_count,
#                     Message.event_id,
#                     Message.user_id,
#                     Message.movie_id,
#                     Message.rating,
#                     Message.rating_timestamp
#                 ])

#         except KeyboardInterrupt:
#             break
    
#     consumer.close()

## 1 Event = 1 Files Result
# Output directory for CSV files
output_directory = "events_output"
os.makedirs(output_directory, exist_ok=True)  # Create the output directory if it doesn't exist

try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue

        Message = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if Message is not None:
            # Print the message to the console
            print("Message record {}: message_count : {}\n"
                  "event_id: {}\n"
                  "user_id: {}\n"
                  "movie_id: {}\n"
                  "rating: {}\n"
                  "rating_timestamp: {}\n"
                  .format(
                      msg.key(),
                      Message.message_count,
                      Message.event_id,
                      Message.user_id,
                      Message.movie_id,
                      Message.rating,
                      Message.rating_timestamp
                  )
            )

            # Create a CSV file for each event based on message_count
            csv_file_path = os.path.join(output_directory, f"event_{Message.message_count}.csv")

            # Write the header row to the CSV file if the file doesn't exist
            if not os.path.isfile(csv_file_path):
                with open(csv_file_path, mode='w', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow(["message_count", "event_id", "user_id", "movie_id", "rating", "rating_timestamp"])

            # Append the data to the CSV file
            with open(csv_file_path, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([
                    Message.message_count,
                    Message.event_id,
                    Message.user_id,
                    Message.movie_id,
                    Message.rating,
                    Message.rating_timestamp
                ])

except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()

