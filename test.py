import os
from dotenv import load_dotenv
from adls import ADLSClient
from kafka import KafkaConsumer
from mysql_client import MySQLClient
from config import EnvConfig
import time

load_dotenv()
config = EnvConfig(os.environ)


def test_adls_client():
    adls_client = ADLSClient(config.storage_account_name, config.storage_account_key)
    test_file_path = "test_upload.txt"
    test_file_name = "test_upload.txt"
    download_path = "test_download.txt"

    with open(test_file_path, "w") as f:
        f.write("This is a test file for ADLS upload.")

    adls_client.upload_file_to_container(
        config.container_name, test_file_path, test_file_name
    )

    adls_client.download_file_from_container(
        config.container_name, test_file_name, download_path
    )

    with open(download_path, "r") as f:
        content = f.read()
        assert content == "This is a test file for ADLS upload."

    os.remove(test_file_path)
    os.remove(download_path)
    print("ADLS Client test passed.")


import time


def test_kafka_consumer():
    kafka_consumer = KafkaConsumer(
        broker=config.kafka_broker,
        group_id=config.kafka_group_id,
        topic=config.kafka_topic,
    )

    timeout = 5
    end_time = time.time() + timeout
    messages = []

    while time.time() < end_time:
        msg_pack = kafka_consumer.consume_messages(timeout=1.0)
        messages.extend(msg_pack)
        for msg in msg_pack:
            print(f"Received message: {msg}")

    kafka_consumer.close()

    assert len(messages) > 0, "No messages received from the topic."
    print(f"Latest message: {messages[-1]}")
    print("Kafka Consumer test passed.")


def test_mysql_client():
    weather_client = MySQLClient(
        config.mysql_host, "weather_admin", config.mysql_password, "weather_db"
    )

    sattelite_client = MySQLClient(
        config.mysql_host, "sattelite_admin", config.mysql_password, "sattelite_db"
    )

    weather_client.connect()
    sattelite_client.connect()

    select_query = "SELECT * FROM satellites LIMIT 10"
    results = sattelite_client.read(select_query)
    for row in results:
        print(row)
    assert len(results) > 0

    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(255) NOT NULL
    )
    """
    weather_client.insert(create_table_query, ())

    insert_query = "INSERT INTO weather_data (data) VALUES (%s)"
    for row in results:
        weather_client.insert(insert_query, (str(row),))

    weather_client.insert("DROP TABLE weather_data", ())
    weather_client.disconnect()
    sattelite_client.disconnect()
    print("MySQL Client test passed.")


if __name__ == "__main__":
    test_adls_client()
    test_kafka_consumer()
    test_mysql_client()
