# Big-Data-Analytics-Project

## Usage

1. Run the FastAPI application:
    ```sh
    uvicorn main:app --reload
    ```

2. The application will be available at `http://localhost:8000`.

## Endpoints

### Fetch Weather Data from Open-Meteo

- **URL:** `/openmeteo`
- **Method:** `POST`
- **Request Body:**
    ```json
    {
        "latitude": 52.52,
        "longitude": 13.405
    }
    ```
- **Response:**
    ```json
    {
        "message": "Weather data sent to NiFi successfully"
    }
    ```

### Fetch Weather Data from OpenWeather

- **URL:** `/openweather`
- **Method:** `POST`
- **Request Body:**
    ```json
    {
        "city": "Berlin"
    }
    ```
- **Response:**
    ```json
    {
        "message": "Weather data sent to NiFi successfully"
    }
    ```

### Fetch Weather Data from WeatherAPI

- **URL:** `/weatherapi`
- **Method:** `POST`
- **Request Body:**
    ```json
    {
        "city": "Berlin"
    }
    ```
- **Response:**
    ```json
    {
        "message": "Weather data sent to NiFi successfully"
    }
    ```

### Fetch Satellite Data

- **URL:** `/satellite`
- **Method:** `POST`
- **Request Body:**
    ```json
    {
        "sat_id": 25544,
        "observer_lat": 52.52,
        "observer_lon": 13.405,
        "observer_alt": 0,
        "seconds": 3600
    }
    ```
- **Response:**
    ```json
    {
        "message": "Satellite data sent to NiFi successfully"
    }
    ```

## Configuration

The application uses environment variables for configuration. The following variables need to be set in the `.env` file:

- `WEATHER_API_KEY`: API key for WeatherAPI.
- `WEATHER_API_URL`: Base URL for WeatherAPI.
- `OPENWEATHER_API_KEY`: API key for OpenWeather.
- `OPENWEATHER_API_URL`: Base URL for OpenWeather.
- `NY2O_API_KEY`: API key for N2YO.
- `NY2O_API_URL`: Base URL for N2YO.
- `OPENMETEO_API_URL`: Base URL for Open-Meteo.
- `NIFI_BASE_URL`: Base URL for NiFi.