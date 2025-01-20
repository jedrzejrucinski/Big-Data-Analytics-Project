import logging
import time
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
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

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
    config.mysql_host, "weather_admin", config.mysql_password, "weather_db"
)

# Create a thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=4)

# Queue for message processing
message_queue = Queue()


def extract_data(message):
    try:
        message = json.loads(message)
        id = int(message["id"])
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

        return id, y, timestamp, x
    except KeyError as e:
        logging.error(f"Missing key in message: {e}")
        return None

def unix_to_hour_pol(time):
    poland_tz = pytz.timezone("Europe/Warsaw")
    return datetime.datetime.fromtimestamp(time, poland_tz).hour


def update_model(model, id, prev_timestamp, timestamp):
    try:
        time_id = timeslot_id(timestamp)
        prev_timestamp_id = timeslot_id(prev_timestamp)

        data = []
        X = []
        Y = []
        seen_timeslot_ids = set()

        # Assume data is fetched here
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

    except Exception as e:
        logging.error(f"Error in update_model: {e}")


def process_message(message):
    try:
        logging.info(f"Processing message: {message}")
        id, y, timestamp, x = extract_data(message)

        if id is None:
            return None

        model_data = adls_client.load_pickled_model_from_container(
            config.container_name, f"model_{id}.pkl"
        )
        model, previous_timestamp, x_hist = (
            model_data["model"],
            model_data["timestamp"],
            model_data["x_hist"],
        )

        x["dt"] = unix_to_hour_pol(x["dt"])

        model.learn_one(y, x_hist[0])

        x_hist = x_hist[1:] + [x]
        forecast = (
            np.clip(np.array(model.forecast(4 * 24, x_hist)), 0, 100)
            .reshape(-1, 4)
            .mean(axis=1)
        )

        update_forecast_to_mysql(id, forecast)
        logging.info(f"Updated forecast for location {id}")
        save_model(id, model, x_hist, forecast)

    except Exception as e:
        logging.error(f"Error processing message: {message}")


def update_forecast_to_mysql(id, forecast):
    try:
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

        with mysql_client as db:
            db.execute(update_query, values)

    except Exception as e:
        logging.error(f"Error updating forecast to MySQL: {e}")


def save_model(id, model, x_hist, timestamp):
    try:
        model_data_pickle = pickle.dumps(
            {"model": model, "timestamp": timestamp, "x_hist": x_hist}
        )
        adls_client.upload_pickle("models", f"model_{id}.pkl", model_data_pickle)

        # Save the forecast in ADLS or other storage
    except Exception as e:
        logging.error(f"Error saving model and forecast: {e}")


def get_location_id(lat, lon):
    try:
        query = "SELECT id FROM locations WHERE latitude = %s AND longitude = %s"
        values = (lat, lon)
        with mysql_client as db:
            result = db.read(query, values)
            if result:
                return result[0]["id"]
            else:
                return None
    except Exception as e:
        logging.error(f"Error fetching location ID: {e}")
        return None


def run_consumer():
    try:
        logging.info("Starting Kafka consumer...")
        while True:
            messages = kafka_consumer.consume_messages(timeout=1.0)
            for message in messages:
                message_queue.put(message)

           #ThreadPoolExecutor
            while not message_queue.empty():
                message = message_queue.get()
                executor.submit(process_message, message)

    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer...")
    finally:
        kafka_consumer.close()
        executor.shutdown()

# def run_consumer():
#     try:
#         logging.info("Starting Kafka consumer...")
#         while True:
#             messages = kafka_consumer.consume_messages(timeout=1.0)
#             for message in messages:
#                 process_message(message)
#     except KeyboardInterrupt:
#         logging.info("Stopping Kafka consumer...")
#     finally:
#         kafka_consumer.close()


if __name__ == "__main__":
    #run_consumer()
    message = {"precipitation":"0.0","cloud_coverage":"4","temperature":"4.45","humidity":"64","wind_speed":"3","wind_direction":"167","id":"918","pressure":"1021","timestamp":"1737375847"}
    process_message(message)
