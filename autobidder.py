import pytz
import pandas as pd
from datetime import datetime
from utils.db import get_database_engine
from utils.google_ads_integration import get_gads_report
from utils.gads_bidchange import GoogleAdsBidChangeExecutor
from utils.date_formatters import num_day_ago2, week_map, num_day_ago
from utils.historical_data import weight_map, get_historical_data, get_old_lh_bid
from utils.calculate_bid import calculate_new_bid, calculate_new_mbid


def autobidder(save_output_on_db=True, change_bid_on_google_ads=True, **kwargs):
    customer_id = kwargs.get('customer_id')
    campaign_id = kwargs.get('campaign_id')
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    max_bid_max = kwargs.get('max_bid_max')
    max_bid_min = kwargs.get('max_bid_min')
    min_bid_max = kwargs.get('min_bid_max')
    min_bid_min = kwargs.get('min_bid_min')
    bid_strategy = kwargs.get('bid_strategy')
    parameter = kwargs.get('parameter')
    mod_percent_max = kwargs.get('mod_percent_max')
    mod_percent_min = kwargs.get('mod_percent_min')
    old_output_table_name = kwargs.get('output_table_name')
    lookback_table_name = kwargs.get('lookback_table_name')

    # Setting Timezone
    tz = pytz.timezone("US/Pacific")
    today = datetime.now(tz)
    weekday = today.weekday()
    hour = today.strftime("%H")
    hour = int(hour)
    today = today.date()
    lw = num_day_ago2(today, 0)
    lasthour = hour - 1


    today_performance_df = get_gads_report(customer_id, campaign_id,
                                           'TODAY_PERFORMANCE_REPORT', start_date, end_date)

    adgroup_performance_df = get_gads_report(customer_id, campaign_id,
                                             'ADGROUP_PERFORMANCE_REPORT', start_date, end_date)

    # Creating a list of currently live adgroups
    current_adgroups = list(adgroup_performance_df['adgroup_name'].unique())

    # Calculating average hourly impressions for ad group
    adgroup_average_impressions = get_gads_report(customer_id, campaign_id, 'ADGROUP_PERFORMANCE_REPORT',
                                                  start_date, end_date, adgroup_names=tuple(current_adgroups))

    historical_data, day_map = get_historical_data(lookback_table_name, campaign_id)
    # historical_data.to_csv('historical_data.csv', sep=',', encoding='utf-8', index=False)
    # historical_data = pd.read_csv('historical_data.csv')

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
    bid_change = movement.reset_index().loc[0, 'distStd']
    lh_bid_change = bid_logic[
        (bid_logic['day_of_week'] == weekday) & (bid_logic['hour_of_day'] == hour - 1)
        ].reset_index().loc[0, 'distStd']

    if hour == 0:
        df_lh = today_performance_df.query("hour_of_day == 23")
    else:
        query = f"day == '{lw}' & hour_of_day == {lasthour}"
        df_lh = today_performance_df.query(query)

    df_lh.reset_index().drop('index', 1)

    impressions = df_lh['impressions']
    latest_imp = sum(impressions)
    if latest_imp < avgimp:
        imp_hoh = (avgimp - latest_imp) / avgimp
        print(imp_hoh)
    else:
        imp_hoh = (latest_imp - avgimp) / avgimp


    lh_bid = get_old_lh_bid(old_output_table_name, campaign_id)
    new_bid = calculate_new_bid(lh_bid_change, imp_hoh, lh_bid)
    new_mbid = calculate_new_mbid(bid_change, new_bid)

    new_change = bid_change - imp_hoh
    new_bid += (new_change * new_bid)

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

    print('new bid direction', bid_change)

    new_bid = round(new_bid, 2)
    new_mbid = round(new_mbid, 2)

    print('new max bid', new_bid)
    print('new min bid', new_mbid)

    max_bid_value = int(new_bid * 1_000_000)
    min_bid_value = int(new_mbid * 1_000_000)

    print(max_bid_value)
    print(min_bid_value)

    if change_bid_on_google_ads:
        bid_change_executor = GoogleAdsBidChangeExecutor(customer_id=customer_id, campaign_id=campaign_id)
        bid_change_executor.execute(bid_strategy=bid_strategy,
                                    parameter=parameter,
                                    max_bid_value=max_bid_value,
                                    min_bid_value=min_bid_value)

    campaign_id = str(adgroup_performance_df.iloc[0]['campaign_id'])

    tmp_dict = {
        'Day': today,
        'Hour': hour,
        'Cost': today_performance_df['cost'].sum(),
        'Avg Imp': avgimp,
        'LH Imp': latest_imp,
        'New bid Direction': bid_change,
        'Old Max Bid': lh_bid,
        'New Max Bid': new_bid,
        'New Min Bid': new_mbid,
        'Bid Strategy': bid_strategy,
        'Parameter': parameter,
        'Campaign': campaign_id
    }
    print(tmp_dict)

    output_df = pd.DataFrame([tmp_dict])
    if save_output_on_db:
        db_engine = get_database_engine()
        output_df.to_sql(kwargs.get('output_table_name'), con=db_engine, if_exists='append', chunksize=1000)
