import hopsworks
import joblib
import matplotlib.pyplot as plt
import pandas as pd
import requests
from settings import *
project = hopsworks.login(project=SETTINGS["FS_PROJECT_NAME"], api_key_value=SETTINGS["FS_API_KEY"])
fs = project.get_feature_store()

import datetime
from datetime import date, timedelta
today = datetime.datetime.now()
yesterday = (today - timedelta(1)).strftime('%Y-%m-%d')
today = today.strftime('%Y-%m-%d')

feature_view = fs.get_feature_view(name="lag_demand_and_weather", version=1)
batch_data = feature_view.get_batch_data(start_time=yesterday, end_time=today)
print("this is batch:", batch_data)

def add_date_features(df):
    new_df = df.copy()
    new_df["day_of_week"] = df['settlement_date'].dt.dayofweek
    new_df["day_of_year"] = df['settlement_date'].dt.dayofyear
    new_df["month"] = df['settlement_date'].dt.month
    new_df["quarter"] = df['settlement_date'].dt.quarter
    new_df["year"] = df['settlement_date'].dt.year
    return new_df

batch_data = add_date_features(batch_data)
batch_data.set_index(['settlement_date'], inplace=True)
batch_data.sort_index(inplace=True)

# Doesn't work since model hasn't been uploaded to model registry yet (see Training.ipynb)
mr = project.get_model_registry()
model = mr.get_model("demand_model", version=1)
model_dir = model.download()
model = joblib.load(model_dir + "/demand_model.pkl")

y_pred = model.predict(batch_data)
# TODO: Do something with this

monitor_fg = fs.get_or_create_feature_group(name="demand_predictions",
                                            version=1,
                                            primary_key=["prediction"],
                                            event_time="settlement_date",
                                            description="Electricity Demand Forecasting Monitoring"
                                            )

demand = y_pred
print("this is y_pred", y_pred)
dates = [pd.Timestamp(today)]
print("this is dates", dates)

data = {
    'prediction': demand,
    'settlement_date': dates
   }

monitor_df = pd.DataFrame(data)
monitor_df['settlement_date'] = monitor_df['settlement_date'].astype("datetime64[ns]")
monitor_fg.insert(monitor_df, write_options={"wait_for_job" : False})

history_df = monitor_fg.read()
print("wjaiosioaj", history_df.head())
print("aepic", monitor_df.head())
# Add our prediction to the history, as the history_df won't have it - 
# the insertion was done asynchronously, so it will take ~1 min to land on App

# Remove time zone information from settlement_date
history_df['settlement_date'] = history_df['settlement_date'].dt.tz_localize(None)

# Convert settlement_date to date only
history_df['settlement_date'] = pd.to_datetime(history_df['settlement_date']).dt.date

# Concatenate history_df and monitor_df
history_df = pd.concat([history_df, monitor_df])

# Convert settlement_date back to Timestamp objects
history_df['settlement_date'] = pd.to_datetime(history_df['settlement_date'])

# Sort DataFrame by settlement_date
history_df.sort_values(by='settlement_date', inplace=True)

print(history_df.head())
print(history_df.dtypes)

fig, ax = plt.subplots(figsize=(15, 5))
history_df.plot(x = 'settlement_date', y = 'prediction',
    style=".", ax=ax, title="Daily One-Step Forecast Plot for England/Wales Demand"
)
plt.ylabel('Electricity Demand (MW)')
plt.xlabel('Date')
plt.legend()
plt.savefig("./historical_forecasts.png")
dataset_api = project.get_dataset_api()    
dataset_api.upload("./historical_forecasts.png", "Resources/images", overwrite=True)
