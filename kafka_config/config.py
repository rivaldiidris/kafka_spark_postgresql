import os
from configparser import ConfigParser

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.ini")
SOURCE_AVRO_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.avsc")
SPOTIFY_CREDS = os.path.join(os.path.dirname(__file__), "spotify_creds.ini")


def read_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read(CONFIG_FILE_PATH)
    return dict(config_parser)


def read_source_avro_schema() -> str:
    with open(SOURCE_AVRO_SCHEMA_PATH) as f:
        return f.read()
    
def read_spotify_creds() -> str:
    spotify_creds_parser = ConfigParser()
    spotify_creds_parser.read(SPOTIFY_CREDS)
    return dict(spotify_creds_parser)