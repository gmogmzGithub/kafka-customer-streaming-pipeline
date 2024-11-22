import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from typing import Optional, Dict, Any, Callable
import signal
import sys
from .config import KAFKA_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaMessageConsumer:
    """
    A class to handle Kafka message consumption with monitoring and processing capabilities.
    """
    
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        processor: Optional[Callable] = None
    ):
        """
        Initialize the Kafka consumer with the given configuration.
        
        Args:
            config (dict, optional): Kafka configuration dictionary.
                                   If None, uses default from config.py
            processor (callable, optional): Function to process received messages
        """
        self.config = config or {
            **KAFKA_CONFIG,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.config)
        self.processor = processor or self._default_processor
        self.running = True
        self._setup_signal_handlers()
        
        # Metrics tracking
        self.metrics = {
            'messages_processed': 0,
            'bytes_processed': 0,
            'start_time': None,
            'errors': 0,
            'last_message_time': None
        }

    def _setup_signal_handlers(self):
        """Set up handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received. Closing consumer...")
        self.running = False

    def _default_processor(self, message: Dict[str, Any]) -> None:
        """
        Default message processor that prints the message.
        
        Args:
            message (dict): Decoded message data
        """
        logger.info(f"Received message: {json.dumps(message, indent=2)}")

    def _update_metrics(self, msg):
        """
        Update processing metrics.
        
        Args:
            msg: Raw message from Kafka
        """
        self.metrics['messages_processed'] += 1
        self.metrics['bytes_processed'] += len(msg.value())
        self.metrics['last_message_time'] = datetime.now()

    def print_metrics(self):
        """Print current processing metrics."""
        if self.metrics['start_time']:
            duration = (datetime.now() - self.metrics['start_time']).total_seconds()
            msg_rate = self.metrics['messages_processed'] / duration if duration > 0 else 0
            
            logger.info("\n=== Consumer Metrics ===")
            logger.info(f"Messages Processed: {self.metrics['messages_processed']}")
            logger.info(f"Bytes Processed: {self.metrics['bytes_processed']} bytes")
            logger.info(f"Runtime: {duration:.2f} seconds")
            logger.info(f"Processing Rate: {msg_rate:.2f} messages/second")
            logger.info(f"Errors Encountered: {self.metrics['errors']}")
            if self.metrics['last_message_time']:
                logger.info(f"Last Message: {self.metrics['last_message_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=====================")

    def consume_messages(
        self,
        topics: list,
        batch_size: int = 100,
        timeout: float = 1.0,
        max_messages: Optional[int] = None
    ):
        """
        Consume messages from specified topics.
        
        Args:
            topics (list): List of topic names to subscribe to
            batch_size (int): Number of messages to process before printing metrics
            timeout (float): Timeout for polling messages in seconds
            max_messages (int, optional): Maximum number of messages to consume
        """
        try:
            self.consumer.subscribe(topics)
            self.metrics['start_time'] = datetime.now()
            logger.info(f"🎯 Starting consumer for topics: {topics}")
            logger.info(f"📊 Batch size: {batch_size}, Timeout: {timeout}s")
            
            while self.running:
                try:
                    msg = self.consumer.poll(timeout=timeout)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        self.metrics['errors'] += 1
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Process message
                    try:
                        message_data = json.loads(msg.value().decode('utf-8'))
                        self.processor(message_data)
                        self._update_metrics(msg)
                        
                        # Print metrics after each batch
                        if self.metrics['messages_processed'] % batch_size == 0:
                            self.print_metrics()
                        
                        # Check if we've reached max_messages
                        if max_messages and self.metrics['messages_processed'] >= max_messages:
                            logger.info(f"Reached maximum message count: {max_messages}")
                            break
                            
                    except json.JSONDecodeError as e:
                        self.metrics['errors'] += 1
                        logger.error(f"Failed to decode message: {e}")
                        continue
                        
                except Exception as e:
                    self.metrics['errors'] += 1
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
        finally:
            self.print_metrics()
            self._cleanup()

    def _cleanup(self):
        """Cleanup resources on shutdown."""
        logger.info("Cleaning up consumer resources...")
        self.consumer.close()
        logger.info("Consumer closed successfully.")

def example_processor(message):
    """
    Example message processor that does some basic analysis.
    
    Args:
        message (dict): Decoded message data
    """
    timestamp = datetime.now().strftime('%H:%M:%S')
    customer_id = message.get('id')
    country = message.get('country')
    company = message.get('company')
    
    logger.info(
        f"[{timestamp}] Processed customer {customer_id} "
        f"from {country} working at {company}"
    )

def main():
    """Main function to demonstrate usage of the KafkaMessageConsumer class."""
    # Create consumer with custom processor
    consumer = KafkaMessageConsumer(processor=example_processor)
    
    try:
        # Start consuming messages
        consumer.consume_messages(
            topics=['customers'],
            batch_size=50,
            timeout=1.0,
            max_messages=1000  # Stop after 1000 messages
        )
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

if __name__ == "__main__":
    main()