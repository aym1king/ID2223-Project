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
n = 3
all_dates = []
for k in range(n, 0, -1):
    date_n_days_ago = (today - timedelta(k)).strftime('%Y-%m-%d')
    all_dates.append(date_n_days_ago)
today = today.strftime('%Y-%m-%d')
all_dates.append(today)
print("all dates:", all_dates)
feature_view = fs.get_feature_view(name="lag_demand_and_weather", version=1)
batch_data = feature_view.get_batch_data()#start_time=all_dates[0], end_time=all_dates[-1])
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
                                            event_time="settlement_date",
                                            description="Electricity Demand Forecasting Monitoring"
                                            )

demand = y_pred
print("this is y_pred", y_pred)
dates = [pd.Timestamp(dt_str) for dt_str in all_dates[:-1]] 
print("this is dates", dates)

data = {
    'prediction': demand,
    'settlement_date': dates
   }

monitor_df = pd.DataFrame(data)
monitor_df['settlement_date'] = monitor_df['settlement_date'].astype("datetime64[ns]")
monitor_fg.insert(monitor_df, write_options={"wait_for_job" : False})

history_df = monitor_fg.read()
# Add our prediction to the history, as the history_df won't have it - 
# the insertion was done asynchronously, so it will take ~1 min to land on App
history_df = pd.concat([history_df, monitor_df])
history_df.set_index('settlement_date')
history_df.sort_index(inplace=True)

history_plot = history_df.set_axis(['england_wales_demand'], axis=1)

fig, ax = plt.subplots(figsize=(15, 5))
history_plot["england_wales_demand"].plot(
    style=".", ax=ax, title="England Wales Demand", label="one-step forecast"
)
plt.legend()
plt.savefig("./historical_forecasts.png")
dataset_api = project.get_dataset_api()    
dataset_api.upload("./historical_forecasts.png", "Resources/images", overwrite=True)
