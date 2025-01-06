from clients.adls import ADLSClient
from clients.kafka import KafkaConsumer
from clients.mysql_client import MySQLClient
from dotenv import load_dotenv
import os
import json
from config import EnvConfig

load_dotenv()

config = EnvConfig(os.environ)

kafka_consumer = KafkaConsumer(
    broker=config.kafka_broker,
    group_id=config.kafka_group_id,
    topic=config.kafka_topic,
)
adls_client = ADLSClient(config.storage_account_name, config.storage_account_key)
mysql_client = MySQLClient(
    config.mysql_host,
    "weather_admin",
    config.mysql_password,
    "weather_db",
)


def process_message(message):
    # Custom logic to process the message

    # need location id here also
    print(f"Processing message: {message}")  # tu jest ML
    print("preprocessing")  # use lat long to get right model pickle file

    # get model
    model = adls_client.load_pickled_model_from_container(
        config.container_name, "initial_model.pkl"
    )

    # fit
    print("fitting model")
    # forecast
    print("forecasting")
    # save forecast to ADLS & mysql
    insert_query = """
        INSERT INTO cloud_cover_forecasts (
            location_id, forecast_hour_1, forecast_hour_2, forecast_hour_3, 
            forecast_hour_4, forecast_hour_5, forecast_hour_6, forecast_hour_7, 
            forecast_hour_8, forecast_hour_9, forecast_hour_10, forecast_hour_11, 
            forecast_hour_12, forecast_hour_13, forecast_hour_14, forecast_hour_15, 
            forecast_hour_16, forecast_hour_17, forecast_hour_18, forecast_hour_19, 
            forecast_hour_20, forecast_hour_21, forecast_hour_22, forecast_hour_23, 
            forecast_hour_24
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    # Example data for insertion
    values = (
        3,  # location_id
        15,
        20,
        25,
        30,
        35,
        40,
        45,
        50,
        55,
        60,
        65,
        70,
        75,
        80,
        85,
        90,
        95,
        100,
        105,
        110,
        115,
        120,
        125,
        130,
    )

    # Execute the query
    with mysql_client as db:
        db.insert(insert_query, values)

    adls_client.upload_dict_as_json(
        config.container_name, "forecast.json", json.dumps(values)
    )  # results are ml forecast results


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
