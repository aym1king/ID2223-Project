# ID2223 Project Electricity Consumption in England/Wales based on Weather Foreceasts

This project uses two APIs with databases, one from https://open-meteo.com/en/docs/historical-weather-api that provides daily weather data by specifying the latitude and longitude of the desired location, and https://www.kaggle.com/datasets/albertovidalrod/electricity-consumption-uk-20092022 that provides hourly updates of average household electricity consumption in the UK. The structure of the project begins with Data_Prep_Backfill that validates the data and joins the weather and electricity consumption datasets on the dates. This daily dataset is then sent to Hopsworks. In Feature_Pipeline the data of the previous weeks of electricity consumption is added as a feature to the featuregroup in Hopsworks. In Training.ipynb further features are added and the model is trained on XGBoost, and the hyperparameters are optimized using GridSearchCV. Finally in the Inference_Pipeline the electricity consumption for today is predicted using data from 2 days before until 1 day before. This is uploaded as a real-time application on Huggingface.co

## Pipelines

**Data_Prep_Backfill**


**Feature_Pipeline**


**Training_Pipeline**


**Inference_Pipeline**


## Hugging Face

_User interface links (Hugging Face)_
    - https://huggingface.co/spaces/Scalable/ID2223-Project-UK-El-Cons

## Created by
Ayman Osman Abubaker and Erland Ekholm
