class EnvConfig:
    weather_api_key: str
    weather_api_url: str
    openweather_api_key: str
    openweather_api_url: str
    ny2o_api_key: str
    ny2o_api_url: str
    openmeteo_api_url: str
    nifi_base_url: str
    storage_account_name: str
    storage_account_key: str
    container_name: str
    kafka_broker: str
    kafka_topic: str
    kafka_group_id: str
    mysql_host: str
    mysql_password: str
    cosmosdb_account_host: str
    cosmosdb_account_key: str
    cosmosdb_database: str
    cosmosdb_container: str

    def __init__(self, config: dict):
        self.weather_api_key = config["WEATHER_API_KEY"]
        self.weather_api_url = config["WEATHER_API_URL"]
        self.openweather_api_key = config["OPENWEATHER_API_KEY"]
        self.openweather_api_url = config["OPENWEATHER_API_URL"]
        self.ny2o_api_key = config["NY2O_API_KEY"]
        self.ny2o_api_url = config["NY2O_API_URL"]
        self.openmeteo_api_url = config["OPENMETEO_API_URL"]
        self.nifi_base_url = config["NIFI_BASE_URL"]
        self.storage_account_name = config["STORAGE_ACCOUNT_NAME"]
        self.storage_account_key = config["STORAGE_ACCOUNT_KEY"]
        self.container_name = config["CONTAINER_NAME"]
        self.kafka_broker = config["KAFKA_BROKER"]
        self.kafka_topic = config["KAFKA_TOPIC"]
        self.kafka_group_id = config["KAFKA_GROUP_ID"]
        self.mysql_host = config["MYSQL_HOST"]
        self.mysql_password = config["MYSQL_PASSWORD"]
        self.cosmosdb_account_host = config["COSMOSDB_ACCOUNT_HOST"]
        self.cosmosdb_account_key = config["COSMOSDB_ACCOUNT_KEY"]
        self.cosmosdb_database = config["COSMOSDB_DATABASE"]
        self.cosmosdb_container = config["COSMOSDB_CONTAINER"]
