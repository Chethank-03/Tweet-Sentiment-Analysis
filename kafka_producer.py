from kafka import KafkaProducer
import json

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to send sentiment results to Kafka topic
def send_to_kafka(tweet_id, sentiment):
    message = {
        'tweet_id': tweet_id,
        'sentiment': sentiment
    }
    producer.send('sentiment_stream', value=message)
    print(f"Sent tweet sentiment result to Kafka: {message}")

# Example usage (sending dummy sentiment data)
send_to_kafka(1, 0.5)  # Sentiment for a tweet with ID 1
send_to_kafka(2, -0.2)  # Sentiment for a tweet with ID 2
