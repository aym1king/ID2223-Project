import os
import modal
from settings import *

LOCAL=False

if LOCAL == False:
   stub = modal.Stub("weather_daily")
   image = modal.Image.debian_slim().pip_install(["hopsworks"]) 

   @stub.function(image=image, schedule=modal.Period(days=1), secret=modal.Secret.from_name("id2223-project-group"))
   def f():
       g()


def get_electricity_demand_and_weather():
    """
    Returns electricity demand and weather DataFrame during the past 30 days starting from the day before yesterday
    """
    import requests

    url = "https://api.nationalgrideso.com/dataset/7a12172a-939c-404c-b581-a6128b74f588/resource/177f6fa4-ae49-4182-81ea- 0c6b35f26ca6/download/demanddataupdate.csv"

    # Request latest electricity demand data
    response = requests.get(url)
    
    if response.status_code == 200:
        with open("demanddataupdate.csv", "wb") as file:
            file.write(response.content)
        print("Resource downloaded successfully.")
    else:
        print("Failed to download the resource.")
    
    import pandas as pd
    df = pd.read_csv("demanddataupdate.csv", index_col=0)
    
    # Change column names to lower case and drop id (row number)
    df.columns = df.columns.str.lower()
    
    # Take only real values and not forecasted ones
    df = df[df['forecast_actual_indicator'] == 'A']
    
    # Take only date and electricity demand columns
    df2 = df[['settlement_period', 'england_wales_demand']]
    
    # Drop rows where settlement_period value is greater than 48
    df2 = df2.drop(index=df2[df2["settlement_period"] > 48].index)
    
    df2.reset_index(inplace=True)
    
    df2.columns = df2.columns.str.lower()
    
    # Drop settlement_period column
    df3 = df2[['england_wales_demand', 'settlement_date']]
    
    # Calculate the daily averages
    k = 48 # Define the value of k which is the number of rows to average, since there are 48 measurements per day
    df3 = df3.groupby(df3.index // k).agg({'settlement_date': 'first', 'england_wales_demand': 'mean'})
    
    df3 = df3.set_index('settlement_date')
    
    df3.index = pd.to_datetime(df3.index)
    
    from datetime import date, timedelta
    past_30_days = []
    for i in range(2, 30):
      past_30_days.append((datetime.datetime.now() - timedelta(i)).strftime('%Y-%m-%d'))
    
    df3 = df3.loc[past_30_days]
    
    demand_df = df3 = df3.iloc[::-1]
    
    import openmeteo_requests
    import requests_cache
    import pandas as pd
    from retry_requests import retry
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    # Receive weather for urban cities with the largest populations in England (London, Birmingham, Manchester) and Wales (Cardiff, Swansea)
    params = {
    	"latitude": [51.5085, 52.4814, 53.4809, 51.48, 51.6208],
    	"longitude": [-0.1257, -1.8998, -2.2374, -3.18, -3.9432],
    	"start_date": past_30_days[-1],
    	"end_date": past_30_days[0],
    	"daily": ["temperature_2m_mean", "sunshine_duration", "precipitation_sum", "precipitation_hours", "wind_speed_10m_max"]
    }
    responses = openmeteo.weather_api(url, params=params)
    
    # Process first location. Add a for-loop for multiple locations or weather models
    weather_dfs = {}
    for i, response in enumerate(responses):
      print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
      print(f"Elevation {response.Elevation()} m asl")
      print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
      print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
    
      # Process daily data. The order of variables needs to be the same as requested.
      daily = response.Daily()
      daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
      daily_sunshine_duration = daily.Variables(1).ValuesAsNumpy()
      daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
      daily_precipitation_hours = daily.Variables(3).ValuesAsNumpy()
      daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()
    
      daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s"),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
      )}
      daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
      daily_data["sunshine_duration"] = daily_sunshine_duration
      daily_data["precipitation_sum"] = daily_precipitation_sum
      daily_data["precipitation_hours"] = daily_precipitation_hours
      daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    
      weather_dfs[i] = pd.DataFrame(data = daily_data)
    
    for key, weather_df in weather_dfs.items():
      weather_dfs[key] = weather_df.set_index('date')
    
    weather_df = pd.concat(weather_dfs.values()).groupby(level=0).mean()
    
    combined_df = pd.concat([demand_df, weather_df], axis=1)
    combined_df.index.rename("settlement_date", inplace=True)
    combined_df.reset_index(inplace=True)
    return combined_df

def g():
    import hopsworks
    import pandas as pd

    project = hopsworks.login(project=SETTINGS["FS_PROJECT_NAME"], api_key_value=SETTINGS["FS_API_KEY"])
    fs = project.get_feature_store()

    weather_df = get_electricity_demand_and_weather()

    weather_fg = fs.get_feature_group(name="weather",version=1)
    weather_fg.insert(weather_df)

if __name__ == "__main__":
    if LOCAL == True :
        g()
    else:
        stub.deploy("weather_daily")
        with stub.run():
            f()
