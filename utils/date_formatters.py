import pandas as pd
from datetime import timedelta


# Function to calculate a prior date given start date and number of days
def num_day_ago(date, num_days):
    date = pd.to_datetime(date)
    new_date = date - timedelta(days=num_days)
    return pd.to_datetime(new_date).tz_localize(None)


# redefining function to work with api
def num_day_ago2(date, num_days):
    date = pd.to_datetime(date)
    new_date = date.date() - timedelta(days=num_days)
    return new_date.strftime('%Y-%m-%d')


# Using start days of week to number weeks
def week_map(x, day_map):
    return_value = None

    try:
        if day_map[1] <= x < day_map[2]:
            return_value = 1
    except:
        pass

    try:
        if day_map[2] <= x < day_map[3]:
            return_value = 2
    except:
        pass

    try:
        if day_map[3] <= x < day_map[4]:
            return_value = 3
    except:
        pass


    try:
        if day_map[4] <= x < day_map[5]:
            return_value = 4
    except:
        pass


    try:
        if day_map[5] <= x < day_map[6]:
            return_value = 5
    except:
        pass


    try:
        if day_map[6] <= x < day_map[7]:
            return_value = 6
    except:
        pass

    try:
        if day_map[7] <= x < day_map[8]:
            return_value = 7
    except:
        pass


    try:
        if day_map[8] <= x < day_map[9]:
            return_value = 8
    except:
        pass

    if return_value is not None:
        return return_value
    else:
        return 1
