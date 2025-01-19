import logging
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
import datetime
import pytz

POF = 1737288000


def timeslot_id(timestamp):
    return math.floor((timestamp - POF) / 60 * 15)


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("azure").setLevel(logging.WARNING)

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


def extract_data(message):
    message = json.loads(message)
    lat, lon = float(message["latitude"]), float(
        message["longitude"]
    )  # this will work but for now it wont match values in the table
    y = int(message["cloud_coverage"])
    timestamp = int(message["timestamp"])

    x = {
        "dt": timestamp,
        "temp": float(message["temperature"]),
        "pressure": int(message["pressure"]),
        "humidity": int(message["humidity"]),
        "wind_speed": float(message["wind_speed"]),
        "wind_deg": int(message["wind_direction"]),
        "precipitation": float(message["precipitation"]),
    }

    return lat, lon, y, timestamp, x


def unix_to_hour_pol(time):
    poland_tz = pytz.timezone("Europe/Warsaw")
    return datetime.datetime.fromtimestamp(time, poland_tz).hour


def update_model(model, id, prev_timestamp, timestamp):
    time_id = timeslot_id(timestamp)
    prev_timestamp_id = timeslot_id(prev_timestamp)

    # get data from hadoop with timestamps between prev_timestamp and timestamp
    data = []
    X = []
    Y = []
    seen_timeslot_ids = set()

    for weather in data:
        ts_id = timeslot_id(weather["dt"])
        if (
            ts_id not in seen_timeslot_ids
            and ts_id > prev_timestamp_id
            and ts_id < time_id
        ):
            seen_timeslot_ids.add(ts_id)
            x = {
                "dt": timestamp,
                "temp": float(weather["temperature"]),
                "pressure": int(weather["pressure"]),
                "humidity": int(weather["humidity"]),
                "wind_speed": float(weather["wind_speed"]),
                "wind_deg": int(weather["wind_direction"]),
                "precipitation": float(weather["precipitation"]),
            }
            y = int(weather["cloud_coverage"])
            X.append(x)
            Y.append(y)

    if len(seen_timeslot_ids) == time_id - prev_timestamp_id - 1:
        logging.info("All data available")
    else:
        logging.warning("Some data missing")

    for x, y in zip(X, Y):
        model.learn_one(y, x)


def process_message(message):
    logging.info(f"Processing message: {message}")
    lat, lon, y, timestamp, x = extract_data(message)

    # lat, lon = 51.7, 19.5
    id = get_location_id(lat, lon)

    ## TODO: check if data is valid and act accordingly

    # get model
    model_data = adls_client.load_pickled_model_from_container(
        config.container_name, f"model_{id}.pkl"
    )

    model, previous_timestamp, x_hist = (
        model_data["model"],
        model_data["timestamp"],
        model_data["x_hist"],
    )

    # TODO: check if model is outdated and act accordingly
    if timeslot_id(timestamp) == timeslot_id(previous_timestamp):
        return
    if timeslot_id(timestamp) < timeslot_id(previous_timestamp):
        logging.info("Old data")
        return
    if timeslot_id(timestamp) != timeslot_id(previous_timestamp) + 1:
        update_model(model, id, previous_timestamp, timestamp)

    # preprocess x
    x["dt"] = unix_to_hour_pol(x["dt"])

    # update model
    model.learn_one(y, x_hist[0])

    # forecast
    x_hist = x_hist[1:] + [x]
    forecast = (
        np.clip(np.array(model.forecast(4 * 24, x_hist)), 0, 100)
        .reshape(-1, 4)
        .mean(axis=1)
    )

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
    values = tuple(forecast) + (id,)

    # Execute the query
    with mysql_client as db:
        db.execute(update_query, values)

    # Save the model back to ADLS and the forecast to ADLS
    model_data_pickle = pickle.dumps(
        {"model": model, "timestamp": timestamp, "x_hist": x_hist}
    )
    adls_client.upload_pickle("models", f"model_{id}.pkl", model_data_pickle)

    forecast_dict = {
        "id": id,
        "timestamp": timestamp,
        "forecast": forecast.tolist(),
    }


def run_consumer():
    try:
        logging.info("Starting Kafka consumer...")
        while True:
            messages = kafka_consumer.consume_messages(timeout=1.0)
            for message in messages:
                process_message(message)
    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer...")
    finally:
        kafka_consumer.close()


def get_location_id(lat, lon):
    query = "SELECT id FROM locations WHERE latitude = %s AND longitude = %s"
    values = (lat, lon)
    with mysql_client as db:
        result = db.read(query, values)
        if result:
            return result[0]["id"]
        else:
            return None


if __name__ == "__main__":
    run_consumer()
