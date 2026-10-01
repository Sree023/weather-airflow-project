from datetime import datetime, timedelta
import requests
import json
import pandas as pd
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
    schedule='*/5 * * * *',  # Keeps your exact 5-minute schedule
    catchup=False
) as dag:

    # 1. Sensor checks the exact URL path that verified successfully
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api', 
        endpoint=f'data/2.5/weather?q=London,uk&APPID={API_KEY}',
        response_check=lambda response: response.status_code == 200,
    )

    # 2. Extract Task (Your original function returning the complete raw JSON)
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
        condition = weather_json['weather'][0]['description'] 
        humidity = weather_json['main']['humidity']
        
        print(f"--- WEATHER REPORT FOR {city_name.upper()} ---")
        print(f"Current Temperature: {temp}°C")
        print(f"Sky Conditions     : {condition.title()}")
        print(f"Humidity Levels    : {humidity}%")
        print("------------------------------------------")
        
        return weather_json

    # 3. Transform Task: Takes raw extracted data, converts units, and creates custom fields
    @task(task_id="transform_weather_data")
    def transform_weather(weather_json: dict):
        main_metrics = weather_json.get('main', {})
        wind_metrics = weather_json.get('wind', {})
        
        celsius_temp = main_metrics.get('temp')
        fahrenheit_temp = round((celsius_temp * 9/5) + 32, 2)
        
        # Determine comfort classification based on humidity threshold rules
        humidity = main_metrics.get('humidity', 0)
        if humidity > 70:
            humidity_profile = 'Humid/Sticky'
        elif humidity < 30:
            humidity_profile = 'Dry'
        else:
            humidity_profile = 'Comfortable'
            
        transformed_data = {
            'city': weather_json.get('name'),
            'country_code': weather_json.get('sys', {}).get('country'),
            'weather_condition': weather_json.get('weather', [{}])[0].get('main'),
            'detailed_description': weather_json.get('weather', [{}])[0].get('description'),
            'temp_celsius': celsius_temp,
            'temp_fahrenheit': fahrenheit_temp,
            'humidity_percentage': humidity,
            'climate_comfort_rating': humidity_profile,
            'wind_speed_mps': wind_metrics.get('speed', 0.0),
            'pressure_hpa': main_metrics.get('pressure'),
            'extracted_at_utc': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"Transform Output: {json.dumps(transformed_data, indent=2)}")
        return transformed_data

    # 4. Load Task: Saves the parsed record directly into your project's CSV data sink
    @task(task_id="load_weather_data")
    def load_weather(clean_data: dict):
        df = pd.DataFrame([clean_data])
        # Directing straight into your active airflow_weather project folder space
        output_file = "/home/ubuntu/airflow_projects/airflow_weather/dags/transformed_weather_data.csv"
        
        file_exists = pd.io.common.file_exists(output_file)
        df.to_csv(output_file, mode='a', index=False, header=not file_exists)
        print(f"Data metrics successfully appended to: {output_file}")

    # Explicit Pipeline Task Dependencies Mapping
    raw_data = fetch_weather_data()
    clean_data = transform_weather(raw_data)
    
    is_weather_api_ready >> raw_data >> clean_data >> load_weather(clean_data)
