import os
import modal

LOCAL=False

if LOCAL == False:
   stub = modal.Stub("weather_daily")
   image = modal.Image.debian_slim().pip_install(["hopsworks"]) 

   @stub.function(image=image, schedule=modal.Period(days=1), secret=modal.Secret.from_name("erland-hopsworks-ai"))
   def f():
       g()


def generate_weather():
    """
    Returns electricity consumption as a single row in a DataFrame
    """
    import pandas as pd
    import random

    df = pd.DataFrame({ })
    #df['quality'] = quality_value
    return df


def get_random_weather():
    """
    Returns a DataFrame
    """
    import pandas as pd
    import random

    return weather_df


def g():
    import hopsworks
    import pandas as pd

    project = hopsworks.login()
    fs = project.get_feature_store()

    weather_df = get_random_weather()

    weather_fg = fs.get_feature_group(name="weather",version=1)
    weather_fg.insert(wine_df)

if __name__ == "__main__":
    if LOCAL == True :
        g()
    else:
        stub.deploy("weather_daily")
        with stub.run():
            f()
