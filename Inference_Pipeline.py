import hopsworks
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

# Doesn't work since model hasn't been updated to model registry yet
mr = project.get_model_registry()
model = mr.get_model("electricity_demand_model", version=1)
model_dir = model.download()
model = joblib.load(model_dir + "/electricity_demand_model.pkl")

y_pred = model.predict(batch_data)
# TODO: Do something with this
