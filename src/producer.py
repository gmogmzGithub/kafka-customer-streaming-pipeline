import json
import time
import logging
from datetime import datetime
from confluent_kafka import Producer
from .config import KAFKA_CONFIG, PRODUCER_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaMessageProducer:
    """
    A class to handle Kafka message production with configurable batching.
    """
    
    def __init__(self, config=None):
        """
        Initialize the Kafka producer with the given configuration.
        
        Args:
            config (dict, optional): Kafka configuration dictionary. 
                                   If None, uses default from config.py
        """
        self.config = config or KAFKA_CONFIG
        self.producer = Producer(self.config)
        
    def delivery_report(self, err, msg):
        """
        Callback function to handle message delivery reports.
        
        Args:
            err: Error message if delivery failed
            msg: Message object containing delivery information
        """
        timestamp = datetime.now().strftime('%H:%M:%S')
        if err is not None:
            logger.error(f'[{timestamp}] ❌ Message delivery failed: {err}')
        else:
            logger.info(
                f'[{timestamp}] ✅ Message {msg.value()[:50]}... '
                f'delivered to {msg.topic()} [partition: {msg.partition()}]'
            )

    def validate_batch_parameters(self, batch_size, batch_pause, message_pause):
        """
        Validate the batch processing parameters against configured limits.
        
        Args:
            batch_size (int): Size of each batch
            batch_pause (float): Pause time between batches
            message_pause (float): Pause time between messages
            
        Returns:
            tuple: Validated batch_size, batch_pause, message_pause
        """
        if batch_size > PRODUCER_CONFIG['max_batch_size']:
            logger.warning(
                f"Batch size {batch_size} exceeds maximum. "
                f"Using {PRODUCER_CONFIG['max_batch_size']}"
            )
            batch_size = PRODUCER_CONFIG['max_batch_size']
            
        if message_pause > PRODUCER_CONFIG['max_message_pause']:
            logger.warning(
                f"Message pause {message_pause} exceeds maximum. "
                f"Using {PRODUCER_CONFIG['max_message_pause']}"
            )
            message_pause = PRODUCER_CONFIG['max_message_pause']
            
        return batch_size, batch_pause, message_pause

    def produce_messages(
        self,
        file_path,
        topic_name,
        batch_size=None,
        batch_pause=None,
        message_pause=None
    ):
        """
        Produce messages to Kafka topic with controlled pacing.
        
        Args:
            file_path (str): Path to the JSON file containing messages
            topic_name (str): Name of the Kafka topic
            batch_size (int, optional): Number of messages per batch
            batch_pause (float, optional): Seconds to pause between batches
            message_pause (float, optional): Seconds to pause between messages
        """
        # Use default values from config if not specified
        batch_size = batch_size or PRODUCER_CONFIG['default_batch_size']
        batch_pause = batch_pause or PRODUCER_CONFIG['default_batch_pause']
        message_pause = message_pause or PRODUCER_CONFIG['default_message_pause']
        
        # Validate parameters
        batch_size, batch_pause, message_pause = self.validate_batch_parameters(
            batch_size, batch_pause, message_pause
        )
        
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                total_messages = len(data)
                
                logger.info(f"\n🚀 Starting to process {total_messages} messages...")
                logger.info(
                    f"📊 Configuration: batch size={batch_size}, "
                    f"batch pause={batch_pause}s, "
                    f"message pause={message_pause}s\n"
                )
                
                for i, record in enumerate(data, 1):
                    try:
                        # Add timestamp to the record for tracking
                        record['produced_at'] = datetime.now().isoformat()
                        
                        # Convert record to JSON string
                        message = json.dumps(record)
                        
                        # Produce the message
                        self.producer.produce(
                            topic_name, 
                            message.encode('utf-8'), 
                            callback=self.delivery_report
                        )
                        
                        # Show progress
                        progress = (i / total_messages) * 100
                        print(f"\rProgress: {progress:.1f}% ({i}/{total_messages})", end='')
                        
                        # Serve delivery callbacks
                        self.producer.poll(0)
                        
                        # Pause between messages
                        time.sleep(message_pause)
                        
                        # If we've reached a batch boundary, take a longer pause
                        if i % batch_size == 0:
                            logger.info(
                                f"\n💤 Pausing for {batch_pause} seconds "
                                f"after batch of {batch_size} messages..."
                            )
                            time.sleep(batch_pause)
                            
                    except Exception as e:
                        logger.error(f"Error processing message {i}: {str(e)}")
                        continue
                
                logger.info("\n\n🔄 Flushing remaining messages...")
                self.producer.flush()
                logger.info("✨ All messages processed successfully!")
                
        except FileNotFoundError:
            logger.error(f"Could not find file: {file_path}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format in file: {file_path}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")

def main():
    """
    Main function to demonstrate usage of the KafkaMessageProducer class.
    """
    producer = KafkaMessageProducer()
    
    # Example usage
    file_path = 'data/customers.json'
    topic_name = 'customers'
    
    producer.produce_messages(
        file_path=file_path,
        topic_name=topic_name,
        batch_size=50,
        batch_pause=3,
        message_pause=0.1
    )

if __name__ == "__main__":
    main()