import os
from configparser import ConfigParser

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.ini")

def read_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read(CONFIG_FILE_PATH)
    return dict(config_parser)\
        
print(read_config())