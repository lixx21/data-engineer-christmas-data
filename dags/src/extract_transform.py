from pyspark.sql import SparkSession
import requests
import os 
from datetime import datetime

def extract_csv_data():

    spark = SparkSession\
        .builder\
        .appName("Christmas Project")\
        .getOrCreate()

    christmas_sales = spark.read.csv('../dags/data/Christmas Sales and Trends.csv', inferSchema=True, header=True)
    christmas_movies = spark.read.csv('../dags/data/christmas_movies.csv', inferSchema=True, header=True)


    return "Success Extract csv data"


def extract_spotify_data():

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    #TODO: Get bearer token
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, headers=headers, data=data)
    bearer_token = response.json()['access_token']

    #TODO: Get data for playlist
    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }

    res_christmas_playlist = requests.get('https://api.spotify.com/v1/search?q=christmas&type=playlist', headers=headers)
    christmas_playlist = res_christmas_playlist.json()

    christmas_playlist_data = []

    for item in christmas_playlist['playlists']['items']:

        if item is not None:
            playlist_name = item['name']
            playlist_description = item['description']
            playlist_url = item['external_urls']['spotify']
            playlist_owner = item['owner']['display_name']
            data_type = item['type']
            playlist_image = item['images'][0]['url']

            christmas_playlist_data.append({
                "type": data_type,
                "name": playlist_name,
                "description": playlist_description,
                "link": playlist_url,
                "owner": playlist_owner,
                "image": playlist_image
            })

    return christmas_playlist_data


def extract_weather_data():

    weather_api_key = os.getenv('WEATHER_API_KEY')

    # Get lat and lon indonesia
    country = "Indonesia"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={country}&appid={weather_api_key}"

    res = requests.get(url)
    data = res.json()

    weather = data['weather'][0]['main']
    weather_description = data['weather'][0]['description']
    temp = data["main"]['temp']
    humidity = data['main']['humidity']
    sea_level = data['main']['sea_level']
    country_name = data['name']
    date_data = int(data['dt'])
    wind_speed = data['wind']['speed']

    weather_data = {
        "weather": weather,
        "weather_description": weather_description,
        "temp": temp,
        "humidity": humidity,
        "sea_level": sea_level,
        "country_name": country_name,
        "date_data": datetime.fromtimestamp(date_data),
        "wind_speed": wind_speed
    }

    return weather_data
