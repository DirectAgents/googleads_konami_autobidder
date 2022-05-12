import pandas as pd
from datetime import datetime
from utils.google_ads_integration import get_gads_report
from utils.date_formatters import num_day_ago2, week_map, num_day_ago
from utils.historical_data import weight_map, get_historical_data
from utils.gads_bidchange import tcpa_max, troas_max, troas_both, tcpa_both, gads_client
from utils.db import get_database_engine

import pytz
from utils.config import get_inputs_config

inputs_config = get_inputs_config()
CAMPAIGN_ID = inputs_config['CAMPAIGN_ID']
OLD_OUTPUT_TABLE_NAME = inputs_config['OLD_OUTPUT_TABLE_NAME']
OUTPUT_TABLE_NAME = inputs_config['OUTPUT_TABLE_NAME']
NUMBER_OF_CAMPAIGNS = inputs_config['NUMBER_OF_CAMPAIGNS']
max_bid_max = inputs_config['MAX_BID_MAX']
max_bid_min = inputs_config['MAX_BID_MIN']
min_bid_max = inputs_config['MIN_BID_MAX']
min_bid_min = inputs_config['MIN_BID_MIN']
bid_strategy = inputs_config['BID_STRATEGY']
parameter = inputs_config['PARAMETER']
mod_percent_max = inputs_config['MOD_PERCENT_MAX']
mod_percent_min = inputs_config['MOD_PERCENT_MIN']
max_top_off = inputs_config['MAX_TOP_OFF']

# Setting Timezone
tz = pytz.timezone("US/Pacific")
today = datetime.now(tz)
weekday = today.weekday()
hour = today.strftime("%H")
hour = int(hour)
today = today.date()
lw = num_day_ago(today, 0)
lasthour = hour - 1


def process(start_date: str, end_date: str):
    today_performance_df = get_gads_report('TODAY_PERFORMANCE_REPORT', start_date, end_date)
    # today_performance_df = pd.read_csv('mocks/today_performance.csv')

    adgroup_performance_df = get_gads_report('ADGROUP_PERFORMANCE_REPORT', start_date, end_date)
    # adgroup_performance_df = pd.read_csv('mocks/adgroup_performance.csv')

    # Creating a list of currently live adgroups
    current_adgroups = list(adgroup_performance_df['adgroup_name'].unique())

    # Calculating average hourly impressions for ad group
    adgroup_average_impressions = get_gads_report('ADGROUP_PERFORMANCE_REPORT',
                                                  start_date, end_date,
                                                  adgroup_names=tuple(current_adgroups))

    # adgroup_performance_df = pd.read_csv('mocks/adgroup_average_impressions.csv')

    historical_data, day_map = get_historical_data(start_date, end_date)

    adgroup_average_impressions['day'] = pd.to_datetime(adgroup_average_impressions['day'])
    adgroup_average_impressions = adgroup_average_impressions.groupby(
        ['campaign_id', 'day', 'hour_of_day']
    ).sum().reset_index()

    adgroup_average_impressions['week_num'] = adgroup_average_impressions.apply(lambda x: week_map(x['day'], day_map),
                                                                                1)
    adgroup_average_impressions = adgroup_average_impressions[adgroup_average_impressions['week_num'] != 'error']
    adgroup_average_impressions = adgroup_average_impressions.groupby(['week_num']).mean().reset_index()[
        ['week_num', 'impressions']
    ]
    adgroup_average_impressions['weights'] = adgroup_average_impressions.apply(lambda x: weight_map[x['week_num']], 1)
    adgroup_average_impressions['weighted_imp'] = adgroup_average_impressions.apply(
        lambda x: x['impressions'] * x['weights'], 1
    )
    avgimp = adgroup_average_impressions['weighted_imp'].sum() / adgroup_average_impressions['weights'].sum()
    adgroup_average_impressions['impressions'].mean()

    bid_logic = historical_data[['day_of_week', 'hour_of_day', 'distStd']]
    # Finding the direction bid should move in for this hour
    movement = bid_logic[(bid_logic['day_of_week'] == weekday) & (bid_logic['hour_of_day'] == hour)]
    bidChange = movement.reset_index().loc[0, 'distStd']
    lh_bidChange = bid_logic[
        (bid_logic['day_of_week'] == weekday) & (bid_logic['hour_of_day'] == hour - 1)
        ].reset_index().loc[0, 'distStd']

    if hour == 0:
        df_lh = today_performance_df[(today_performance_df['hour_of_day'] == '23')]
    else:
        df_lh = today_performance_df[
            (today_performance_df['day'] == lw) & (today_performance_df['hour_of_day'] == lasthour)]

    df_lh.reset_index().drop('index', 1)

    latestimp = 0
    impressions = []
    impressions = df_lh['impressions']
    latestimp = sum(impressions)
    if latestimp < avgimp:
        a = (avgimp - latestimp) / avgimp
        print(a)
    else:
        print('No')

    query = f"""
            SELECT * FROM {OLD_OUTPUT_TABLE_NAME} WHERE [Campaign] = '{CAMPAIGN_ID}'
        """
    lh_bid = pd.read_sql(query, con=get_database_engine())

    # lh_bid = pd.read_csv('mocks/old_autobidder_cw_konami.csv')

    try:
        lh_bid = lh_bid.sort_values(['Day', 'Hour'], ascending=False).reset_index().loc[0, 'New Bid']
    except:
        try:
            lh_bid = lh_bid.sort_values(['Day', 'Hour'], ascending=False).reset_index().loc[0, 'max_bid']
        except:
            lh_bid = None

    if lh_bid is None:
        lh_bid = 0

    imp_hoh = (latestimp - avgimp) / avgimp

    if 0 < lh_bidChange <= 1:
        if 0 > imp_hoh >= -1:
            new_bid = lh_bid + .5
        elif imp_hoh < -1:
            new_bid = lh_bid + 1
        else:
            new_bid = lh_bid
    elif lh_bidChange > 1:
        if 0 > imp_hoh >= -1:
            new_bid = lh_bid + .75
        elif imp_hoh < -1:
            new_bid = lh_bid + 1.25
        else:
            new_bid = lh_bid

    if 0 > lh_bidChange >= -1:
        if 0 < imp_hoh <= 1:
            new_bid = lh_bid - .5
        elif imp_hoh > 1:
            new_bid = lh_bid - 1
        else:
            new_bid = lh_bid
    elif lh_bidChange < -1:
        if 0 < imp_hoh <= 1:
            new_bid = lh_bid - .75
        elif imp_hoh > 1:
            new_bid = lh_bid - 1.25
        else:
            new_bid = lh_bid

    new_mbid = 0.65

    if 0 < bidChange <= .5:
        new_bid += .25
        new_mbid += .05
    elif .5 < bidChange <= 1:
        new_bid += .5
        new_mbid += .07
    elif 1 < bidChange <= 1.5:
        new_bid += .75
        new_mbid += .1
    elif 1.5 < bidChange <= 2:
        new_bid += 1
        new_mbid += .15
    elif 2 < bidChange <= 2.5:
        new_bid += 1.25
        new_mbid += .2
    elif bidChange > 2.5:
        new_bid += 1.5
        new_mbid += .25

    elif 0 >= bidChange >= -.5:
        new_bid -= .25
        new_mbid -= .05
    elif -.5 > bidChange >= -1:
        new_bid -= .5
        new_mbid -= .07
    elif -1 > bidChange >= -1.5:
        new_bid -= .75
        new_mbid -= .1
    elif -1.5 > bidChange >= -2:
        new_bid -= 1
        new_mbid -= .15
    elif -2 > bidChange >= -2.5:
        new_bid -= 1.25
        new_mbid -= .2
    elif bidChange < -2.5:
        new_bid -= 1.5
        new_mbid -= .5

    newChange = bidChange - imp_hoh

    new_bid += (newChange * new_bid)

    if mod_percent_max > 0:
        new_bid += (mod_percent_max * new_bid)
    elif mod_percent_max < 0:
        new_bid -= (mod_percent_max * new_bid)
    else:
        new_bid = new_bid

    if mod_percent_min > 0:
        new_bid += (mod_percent_min * new_bid)
    elif mod_percent_min < 0:
        new_bid -= (mod_percent_min * new_bid)
    else:
        new_bid = new_bid

    print('original new bid', new_bid, new_mbid)

    # Bring down to the right
    if new_bid <= max_bid_min:
        new_bid = max_bid_min
    if new_bid >= max_bid_max:
        new_bid = max_bid_max

    if new_mbid <= min_bid_min:
        new_mbid = min_bid_min
    if new_mbid >= min_bid_max:
        new_mbid = min_bid_max

    print('last hour bid', lh_bid)

    print('impressions hour over hour', imp_hoh)

    print('new bid direction', bidChange)

    new_bid = round(new_bid, 2)
    new_mbid = round(new_mbid, 2)

    print('new max bid', new_bid)
    print('new min bid', new_mbid)

    max_bid_value = int(new_bid * 1_000_000)
    min_bid_value = int(new_mbid * 1_000_000)

    print(max_bid_value)
    print(min_bid_value)

    # if bid_strategy == 'tcpa' and parameter == 'max':
    #     tcpa_max(gads_client, max_bid_value)
    # elif bid_strategy == 'tcpa' and parameter == 'both':
    #     tcpa_both(gads_client, max_bid_value, min_bid_value)
    # elif bid_strategy == 'troas' and parameter == 'max':
    #     troas_max(gads_client, max_bid_value)
    # else:
    #     troas_both(gads_client, max_bid_value, min_bid_value)

    campaign_id = str(adgroup_performance_df.iloc[0]['campaign_id'])

    tmp_dict = {
        'Day': today,
        'Hour': hour,
        'Cost': today_performance_df['cost'].sum(),
        'Avg Imp': avgimp,
        'LH Imp': latestimp,
        'New bid Direction': bidChange,
        'Old Max Bid': lh_bid,
        'New Max Bid': new_bid,
        'New Min Bid': new_mbid,
        'Bid Strategy': bid_strategy,
        'Parameter': parameter,
        'Campaign': campaign_id
    }
    print(tmp_dict)

    output_df = pd.DataFrame([tmp_dict])
    return output_df


if __name__ == '__main__':
    # used to find the date 77 days ago and once you run it manually change 77 to 7 and have it scheduled to run
    # every morning once
    st_date = num_day_ago2(datetime.today().date(), 7)

    # st_date = '2022-04-22'

    # used to find the date same time last week
    ed_date = num_day_ago2(datetime.today().date(), 0)

    data = process(st_date, ed_date)
    data.to_csv('autobidder_output.csv', encoding='utf-8')

    db_engine = get_database_engine()
    data.to_sql(OUTPUT_TABLE_NAME, con=db_engine, if_exists='append', chunksize=1000)
