# ID2223 Project Electricity Consumption in England/Wales based on Weather Foreceasts

This project uses two APIs with databases, one from https://open-meteo.com/en/docs/historical-weather-api that provides daily weather data by specifying the latitude and longitude of the desired location, and https://www.kaggle.com/datasets/albertovidalrod/electricity-consumption-uk-20092022 that provides hourly updates of average household electricity consumption in the UK. The structure of the project begins with Data_Prep_Backfill which validates the data and joins the weather and electricity consumption datasets on the dates. This daily dataset is then sent to Hopsworks. In Feature_Pipeline the data of the previous weeks of electricity consumption is added as a feature to the featuregroup in Hopsworks. In Training.ipynb further features are added and the model is trained on XGBoost, and the hyperparameters are optimized using GridSearchCV. Finally in the Inference_Pipeline the electricity consumption for today is predicted using data from 2 days before until 1 day before. This is uploaded as a real-time application on Huggingface.co

## Pipelines

**Data_Prep_Backfill**
In this file, we read the dataset from the weather API by specifying the latitude and longitude of the locations we want to gather the data from. In our case, we chose the largest cities in England and Wales, which are London, Birmingham, Manchester in England, and Cardiff, Swansea in Wales. The reason that the biggest cities are only chosen from England and Wales is that you also specify the location for the electricity consumption, which in our case is England and Wales. The dataset was validated by checking that there weren't any missing values and some adjustments were made such as providing names of the features in the dataset and adding the exact date before pushing it into Hopsworks.

**Feature_Pipeline**
Here the lagged features are created in a span of 7, 14, 21, and 28 days which provides added information to the feature group in Hopsworks.

**Training_Pipeline**
Before training the model further features are added (day of week, day of year, month, quarter, year) which can provide further information for the model to train on. The model was trained on XGBoost because it is an accurate time-series forecasting model, and the hyperparameters were tuned using grid search. However, the test results are high so some adjustments need to be made.

**Inference_Pipeline**
In the inference pipeline, we provide daily updated batch data on the predicted electricity demand based on the span between 2 days before and 1 day before. This graph is then uploaded to Huggingface.co

## Hugging Face

_User interface links (Hugging Face)_
    - https://huggingface.co/spaces/Scalable/ID2223-Project-UK-El-Cons

## Created by
Ayman Osman Abubaker and Erland Ekholm
