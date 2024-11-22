# Kafka Data Streaming Pipeline Demo

## Overview
This project demonstrates a real-time data streaming pipeline using Apache Kafka and Python, showcasing common data engineering practices and patterns. The implementation features a configurable producer that streams customer data with controlled throughput and monitoring capabilities.

## 🛠 Technical Stack
- Python 3.x
- Apache Kafka
- Confluent Kafka Python Client
- JSON Data Processing

## 🏗 Architecture
The pipeline consists of:
1. **Data Source**: JSON file containing customer records
2. **Producer**: Configurable Kafka producer with:
   - Batch processing capabilities
   - Throughput control
   - Real-time monitoring
   - Delivery confirmations
3. **Error Handling**: Comprehensive error reporting and delivery tracking
4. **Monitoring**: Real-time progress tracking and status updates

## 📊 Features
- **Configurable Batch Processing**: Control message batching size and timing
- **Throttling Capabilities**: Prevent system overload with configurable pauses
- **Real-time Monitoring**: Track progress and delivery status
- **Timestamp Tracking**: Monitor message timing and latency
- **Error Handling**: Robust error catching and reporting
- **Progress Visualization**: Real-time progress updates and batch completion tracking

## 🚀 Getting Started

### Prerequisites
```bash
pip install confluent-kafka
```

### Configuration
Update the `config` dictionary in the producer script with your Kafka cluster details:
```python
config = {
    'bootstrap.servers': 'your-server:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'your-username',
    'sasl.password': 'your-password'
}
```

### Running the Producer
```python
from kafka_producer import produce_messages

produce_messages(
    file_path='customer.json',
    topic_name='customers',
    batch_size=50,      # Messages per batch
    batch_pause=3,      # Seconds between batches
    message_pause=0.1   # Seconds between messages
)
```

## 📝 Example Output
```
🚀 Starting to process 9,999 messages...
📊 Configuration: batch size=50, batch pause=3s, message pause=0.1s

[10:15:23] ✅ Message {"id":1,"email":"user@example.com"...} delivered to customers [partition: 0]
Progress: 0.5% (50/9999)
💤 Pausing for 3 seconds after batch of 50 messages...
```

## 🔧 Customization
Adjust the producer parameters to match your use case:
```python
# Detailed observation mode
produce_messages(file_path, topic_name, batch_size=10, batch_pause=5, message_pause=1)

# Production mode
produce_messages(file_path, topic_name, batch_size=100, batch_pause=1, message_pause=0.05)
```

## 📈 Future Enhancements
- [ ] Add Kafka consumer implementation
- [ ] Implement data transformation layers
- [ ] Add metrics collection and monitoring dashboard
- [ ] Implement schema validation
- [ ] Add unit and integration tests

## 🤝 Contributing
Feel free to fork this repository and submit pull requests. You can also open issues for any bugs or improvements.
