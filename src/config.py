import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD')
}

# Producer configuration
PRODUCER_CONFIG = {
    'default_batch_size': 50,
    'default_batch_pause': 3,
    'default_message_pause': 0.1,
    'max_batch_size': 1000,
    'max_message_pause': 10
}