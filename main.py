from clients.adls import ADLSClient
from clients.kafka import KafkaConsumer
from dotenv import load_dotenv
import os
from config import EnvConfig

load_dotenv()

config = EnvConfig(os.environ)

kafka_consumer = KafkaConsumer(
    broker=config.kafka_broker,
    group_id=config.kafka_group_id,
    topic=config.kafka_topic,
)


def process_message(message):
    # Custom logic to process the message
    print(f"Processing message: {message}")
    # Add your custom logic here


def run_consumer():
    try:
        print("Starting Kafka consumer...")
        while True:
            messages = kafka_consumer.consume_messages(timeout=1.0)
            for message in messages:
                process_message(message)
    except KeyboardInterrupt:
        print("Stopping Kafka consumer...")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    run_consumer()
