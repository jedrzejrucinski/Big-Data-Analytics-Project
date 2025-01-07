from clients.adls import ADLSClient
from clients.kafka import KafkaConsumer
from clients.mysql_client import MySQLClient
from dotenv import load_dotenv
import os
import json
from config import EnvConfig
import numpy as np
import pickle
import math

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

    print(f"Processing message: {message}")  # tu jest 
    message = json.loads(message)
    print(type(message['latitude']))
    #lat, lon = float(message['latitude']), float(message['longitude'])  this will work but for now it wont match values in the table
    lat, lon = 51.7, 19.5
    id = get_location_id(lat, lon)
    print(f"Location id: {id}")
    
    print("preprocessing")  # use lat long to get right model pickle file
    y = message["cloud_coverage"]
    # preprocessing X here, not relevant right now

    # get model
    model = adls_client.load_pickled_model_from_container(
        config.container_name, "initial_model.pkl"
    )
    # fit
    print("fitting model")
    model.learn_one(int(y))
    
    # forecast
    print("forecasting")
    forecast = forecast = np.clip(np.array(model.forecast(4*24)), 0, 100).reshape(-1, 4).mean(axis=1)

    # save forecast to ADLS & mysql
    update_query = """
        UPDATE cloud_cover_forecasts SET
            forecast_hour_1 = %s, forecast_hour_2 = %s, forecast_hour_3 = %s, 
            forecast_hour_4 = %s, forecast_hour_5 = %s, forecast_hour_6 = %s, 
            forecast_hour_7 = %s, forecast_hour_8 = %s, forecast_hour_9 = %s, 
            forecast_hour_10 = %s, forecast_hour_11 = %s, forecast_hour_12 = %s, 
            forecast_hour_13 = %s, forecast_hour_14 = %s, forecast_hour_15 = %s, 
            forecast_hour_16 = %s, forecast_hour_17 = %s, forecast_hour_18 = %s, 
            forecast_hour_19 = %s, forecast_hour_20 = %s, forecast_hour_21 = %s, 
            forecast_hour_22 = %s, forecast_hour_23 = %s, forecast_hour_24 = %s
        WHERE location_id = %s
    """

    # Example data for insertion
    values = (id,) + tuple(forecast)

    # Execute the query
    with mysql_client as db:
        db.execute(update_query, values)


    # Save the model back to ADLS and the forecast to ADLS
    # model_pickle = pickle.dumps(model)
    # adls_client.upload_pickle(
    #     config.container_name, "initial_model.pkl", model_pickle
    # )

    # adls_client.upload_dict_as_json(
    #     config.container_name, "forecast.json", json.dumps(values+(message['timestamp']))
    # )  


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


def get_location_id(lat, lon):
    query = "SELECT id FROM locations WHERE latitude = %s AND longitude = %s"
    values = (lat, lon)
    with mysql_client as db:
        result = db.read(query, values)
        if result:
            return result[0]['id']
        else:
            return None

if __name__ == "__main__":
    run_consumer()
