class EnvConfig:
    weather_api_key: str
    weather_api_url: str
    openweather_api_key: str
    openweather_api_url: str
    ny2o_api_key: str
    ny2o_api_url: str
    openmeteo_api_url: str
    nifi_base_url: str

    def __init__(self, config: dict):
        self.weather_api_key = config["WEATHER_API_KEY"]
        self.weather_api_url = config["WEATHER_API_URL"]
        self.openweather_api_key = config["OPENWEATHER_API_KEY"]
        self.openweather_api_url = config["OPENWEATHER_API_URL"]
        self.ny2o_api_key = config["NY2O_API_KEY"]
        self.ny2o_api_url = config["NY2O_API_URL"]
        self.openmeteo_api_url = config["OPENMETEO_API_URL"]
        self.nifi_base_url = config["NIFI_BASE_URL"]
