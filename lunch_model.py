import requests
import json
import sys
import time
import fastavro
from train_batch import train_model
from clients.adls import ADLSClient
import pickle
from dotenv import load_dotenv
from config import EnvConfig
import os
from clients.mysql_client import MySQLClient

load_dotenv()

config = EnvConfig(os.environ)

adls_client = ADLSClient(config.storage_account_name, config.storage_account_key)

mysql_client = MySQLClient(
    config.mysql_host,
    "weather_admin",
    config.mysql_password,
    "weather_db",
)

def get_historical_weather():
    end_time = int(time.time())
    start_time = end_time - 60*60*24*364
    data = []
    counter = 0
    while start_time < end_time:
        print(counter)
        counter += 1
        if counter > 100:
            print("API request limit reached. Exiting.")
            break
        api_key = config.openweather_api_key
        api_url = f"https://history.openweathermap.org/data/2.5/history/city?lat=51.7&lon=19.5&type=hour&start={start_time}&end={end_time}&units=metric&appid={api_key}"
        response = requests.get(api_url)
        # Check if the request was successful
        if response.status_code == 200:
            partial_data = response.json()['list']
        else:
            print(f"Failed to fetch API data. Status code: {response.status_code}, Message: {response.text}")
            sys.exit(1)
        start_time = partial_data[-1]['dt'] + 3600
        data.extend(partial_data)
        time.sleep(0.01)


    # with open("output.json", "w") as file:
    #     json.dump(data, file, indent=4)
    # print("API output saved to output.json")
    return data

def save_avro(data):
    data_new = []
    for d in data:
        d_new = {}
        d_new['dt'] = d['dt']
        d_new['temp'] = d['main']['temp']
        d_new['pressure'] = d['main']['pressure']
        d_new['humidity'] = d['main']['humidity']
        d_new['wind_speed'] = d['wind']['speed']
        d_new['wind_deg'] = d['wind']['deg']
        d_new['precipitation'] = 0.0
        if 'rain' in d:
            d_new['precipitation'] += d['rain']['1h']
        if 'snow' in d:
            d_new['precipitation'] += d['snow']['1h']
        d_new['clouds'] = d['clouds']['all']
        data_new.append(d_new)

            # Define the Avro schema
    schema = {
        "type": "record",
        "name": "WeatherData",
        "fields": [
            {"name": "dt", "type": "long"},
            {"name": "temp", "type": "float"},
            {"name": "pressure", "type": "int"},
            {"name": "humidity", "type": "int"},
            {"name": "wind_speed", "type": "float"},
            {"name": "wind_deg", "type": "int"},
            {"name": "precipitation", "type": "float"},
            {"name": "clouds", "type": "int"}
        ]
    }

    # Save the DataFrame as Avro
    avro_path = "weatherbatch.avro"
    with open(avro_path, "wb") as out_file:
        fastavro.writer(out_file, schema, data_new)

def send_to_adls(model_data, id):
    model_data_pickle = pickle.dumps(model_data)
    adls_client.upload_pickle("models", f"model_{id}.pkl", model_data_pickle)

def get_location_ids():
    query = "SELECT id FROM locations"
    with mysql_client as db:
        result = db.read(query, ())
        if result:
            return [row['id'] for row in result]
        else:
            return None

if __name__ == "__main__":
    #data = get_historical_weather()
    #save_avro(data)
    model_data = train_model()
    ids = get_location_ids()
    counter = 0
    for id in ids:
        counter += 1
        if counter % 100 == 0:
            print(f"Trained {counter} models")
        send_to_adls(model_data, id)
