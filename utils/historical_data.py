import pytz
import pandas as pd
from datetime import timedelta, datetime
from utils.db import get_database_engine
from utils.date_formatters import num_day_ago, week_map


# Setting Timezone
tz = pytz.timezone("US/Pacific")
today = datetime.now(tz)
weekday = today.weekday()
hour = today.strftime("%H")
hour = int(hour)
today = today.date()

# Creating map of week number and weight
weight_map = {
    8: .2,
    7: .2,
    6: .2,
    5: .1,
    4: .1,
    3: .1,
    2: .05,
    1: .05
}


def clean_historical_data(dataframe: pd.DataFrame, start_date=None, end_date=None) -> pd.DataFrame:
    tz = pytz.timezone("US/Pacific")
    today = datetime.now(tz)

    cleaned_data = dataframe
    cleaned_data.drop_duplicates(['day', 'hour_of_day'], inplace=True)
    cleaned_data['day'] = pd.to_datetime(cleaned_data['day'])

    if not start_date and end_date:
        # Creating start and end date to filter historical data (Past 8 weeks not including this week)
        start_date = pd.to_datetime(num_day_ago(today, 1 + today.weekday())) - timedelta(days=1)
        end_date = pd.to_datetime(num_day_ago(today, today.weekday()))  # - timedelta(days=1)
    else:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

    # Filtering DataFrame
    cleaned_data = cleaned_data[
        (cleaned_data['day'].dt.date >= start_date)
        &
        (cleaned_data['day'].dt.date <= end_date)
        ]
    cleaned_data.sort_values('day', inplace=True)

    return cleaned_data


def get_day_map(dataframe: pd.DataFrame) -> dict:
    # Creating dictionary of all start days of week (Mondays)
    day_map = {}
    i = 1
    for day in dataframe[dataframe['day_of_week'] == 0].day.unique():
        day_map[i] = day
        i += 1

    day_map[9] = dataframe.day.max()
    return day_map


def get_historical_data(lookback_table_name: str, campaign_id: str,
                        start_date: str, end_date: str) -> (pd.DataFrame, dict):
    historical_data_query = f"""
        SELECT * FROM {lookback_table_name} WHERE [Campaign ID] = '{campaign_id}'
    """

    database_engine = get_database_engine()
    dataframe = pd.read_sql(historical_data_query, database_engine)
    # dataframe = pd.read_csv('mocks/lookback_output.csv')

    dataframe = dataframe.rename(columns={
        'Campaign': 'campaign',
        'Campaign ID': 'campaign_id',
        'Day': 'day',
        'Day of week': 'day_of_week',
        'Hour of day': 'hour_of_day',
        'Impressions': 'impressions',
        'Clicks': 'clicks',
        'Cost': 'cost',
        'Conversions': 'conversions',
        'Total conv. value': 'conversions_value',
        'Search Impr. share': 'search_impression_share',
        'ROAS': 'roas',
        'CPC': 'cpc',
        'Budget': 'budget'
    })

    cleaned_data = clean_historical_data(dataframe, start_date, end_date)

    day_map = get_day_map(cleaned_data)

    # Adding Week nums to dataframe
    cleaned_data['week_num'] = cleaned_data.apply(lambda x: week_map(x['day'], day_map), 1)

    cleaned_data = cleaned_data[
        (cleaned_data['week_num'] != 'error') & (cleaned_data['week_num'] != 9)
        ].sort_values('day')

    # Grouping by hour of day, day of week, and week number to create weighted averages
    df_hour = cleaned_data.groupby(['hour_of_day', 'day_of_week', 'week_num']).mean().reset_index()

    df_hour = df_hour.groupby(['hour_of_day', 'day_of_week', 'week_num']).mean().reset_index()

    df_hour = df_hour.sort_values('day_of_week')

    df_hour['weights'] = df_hour.apply(lambda x: weight_map[x['week_num']], 1)

    df_hour['weighted_conv'] = df_hour.apply(lambda x: x['conversions'] * x['weights'], 1)

    df_hour_gb = df_hour.groupby(['hour_of_day', 'day_of_week']).sum().reset_index()

    conv_hour = df_hour_gb['weighted_conv'].sum() / df_hour_gb['weights'].sum()

    std_hour = df_hour_gb['weighted_conv'].std()

    df_hour_gb.pivot(index='hour_of_day', columns='day_of_week', values='weighted_conv')

    df_hour_gb['distMean'] = df_hour_gb.apply(lambda x: x['weighted_conv'] - conv_hour, 1)

    df_hour_gb['distStd'] = df_hour_gb.apply(lambda x: x['distMean'] / std_hour, 1)

    df_hour_gb.pivot(index='hour_of_day', columns='day_of_week', values='distStd')
    return df_hour_gb, day_map


def get_old_lh_bid(old_output_table_name: str, campaign_id: str):

    try:
        query = f"""
                   SELECT * FROM {old_output_table_name} WHERE [Campaign] = '{campaign_id}'
               """
        lh_bid_dataframe = pd.read_sql(query, con=get_database_engine())
        lh_bid = lh_bid_dataframe.sort_values(['Day', 'Hour'], ascending=False).reset_index().loc[0, 'New Max Bid']
    except Exception as ex:
        print(ex)
        lh_bid = None

    if lh_bid is None:
        lh_bid = 0

    return lh_bid
