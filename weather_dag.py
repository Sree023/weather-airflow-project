from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor

# Verified Credentials from your working query
API_KEY = 'bf4a710d5ee29bdafb443664a96d51ce'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'weather_practice_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    # 1. Sensor checks the exact URL path that verified successfully
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api', 
        endpoint=f'data/2.5/weather?q=London,uk&APPID={API_KEY}',
        response_check=lambda response: response.status_code == 200,
    )

    # 2. The TaskFlow function extracts data and logs it safely
    @task(task_id="extract_and_log_weather")
    def fetch_weather_data():
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': 'London,uk',
            'APPID': API_KEY,
            'units': 'metric'  # Converts Kelvin to readable Celsius
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        weather_json = response.json()
        
        city_name = weather_json.get('name', 'Unknown')
        temp = weather_json['main']['temp']
        
        # FIXED: Added [0] index because 'weather' is a list containing a dictionary
        condition = weather_json['weather'][0]['description'] 
        humidity = weather_json['main']['humidity']
        
        print(f"--- WEATHER REPORT FOR {city_name.upper()} ---")
        print(f"Current Temperature: {temp}°C")
        print(f"Sky Conditions     : {condition.title()}")
        print(f"Humidity Levels    : {humidity}%")
        print("------------------------------------------")
        
        return weather_json

    is_weather_api_ready >> fetch_weather_data()