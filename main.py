import threading
import pandas as pd
from textwrap import dedent
from datetime import datetime
from autobidder import autobidder
from utils.date_formatters import num_day_ago2
import sys
import warnings

warnings.filterwarnings("ignore")


def main(**kwargs):
    try:
        print(dedent(f"""
                Running Autobidder for:
                Customer: {kwargs.get('customer_name')}/{kwargs.get('customer_id')}
                Campaign: {kwargs.get('campaign_name')}/{kwargs.get('campaign_id')}
                Output Table will be: {kwargs.get('output_table_name')}
            """))
        autobidder(
            **kwargs
        )
        print(dedent(f"""
                Success Running for:
                Customer: {kwargs.get('customer_name')}/{kwargs.get('customer_id')}
                Campaign: {kwargs.get('campaign_name')}/{kwargs.get('campaign_id')}
                Output: {kwargs.get('output_table_name')}
            """))
    except Exception as ex:
        print(ex)
        print(dedent(f"""
                Failed Running for:
                Customer: {kwargs.get('customer_name')}/{kwargs.get('customer_id')}
                Campaign: {kwargs.get('campaign_name')}/{kwargs.get('campaign_id')}
        """))


class AutobidderGroupThread:
    def __init__(self, campaigns_group: list):
        self.campaigns = campaigns_group
        thread = threading.Thread(target=self.autobidder)
        thread.start()

    def autobidder(self):
        for campaign in self.campaigns:
            main(**campaign)


if __name__ == '__main__':
    campaigns = pd.read_csv('campaign_input.csv', dtype={
        'Customer Id': str,
        'Campaign Id': str
    })
    campaigns = campaigns.rename(columns={
        'Account': 'customer_name',
        'Campaign': 'campaign_name',
        'Customer Id': 'customer_id',
        'Campaign Id': 'campaign_id',
        'Input Table': 'lookback_table_name',
        'Output Table': 'output_table_name',
        'Number of Campaigns': 'number_of_campaigns',
        'Bid Strategy': 'bid_strategy',
        'Parameter': 'parameter',
        'Mod Percent Max': 'mod_percent_max',
        'Mod Percent Min': 'mod_percent_min',
        'Max Bid Max': 'max_bid_max',
        'Max Bid Min': 'max_bid_min',
        'Min Bid Max': 'min_bid_max',
        'Min Bid Min': 'min_bid_min',
        'Status': 'status'
    }).query(
        'status != "paused"'
    ).reset_index()

    # get list of customers
    customers_list = list(campaigns['customer_name'].unique())

    # create different lists grouped by customer_name
    customers_campaigns = [
        campaigns[campaigns['customer_name'] == customer]
        for customer in customers_list
    ]

    start_date = num_day_ago2(datetime.today().date(), 7)
    end_date = num_day_ago2(datetime.today().date(), 0)

    group_of_campaigns_to_run = [
        [{'customer_id': customer_id,
          'campaign_id': campaign_id,
          'customer_name': customer_name,
          'campaign_name': campaign_name,
          'output_table_name': output_table_name,
          'lookback_table_name': lookback_table_name,
          'number_of_campaigns': number_of_campaigns,
          'bid_strategy': bid_strategy,
          'parameter': parameter,
          'mod_percent_max': mod_percent_max,
          'mod_percent_min': mod_percent_min,
          'max_bid_max': max_bid_max,
          'max_bid_min': max_bid_min,
          'min_bid_max': min_bid_max,
          'min_bid_min': min_bid_min,
          'start_date': start_date,
          'end_date': end_date}
         for customer_id, campaign_id, customer_name, campaign_name, lookback_table_name,
             output_table_name, number_of_campaigns, bid_strategy, parameter, mod_percent_max, mod_percent_min,
             max_bid_max, max_bid_min, min_bid_max, min_bid_min
         in zip(customer_campaign['customer_id'],
                customer_campaign['campaign_id'],
                customer_campaign['customer_name'],
                customer_campaign['campaign_name'],
                customer_campaign['lookback_table_name'],
                customer_campaign['output_table_name'],
                customer_campaign['number_of_campaigns'],
                customer_campaign['bid_strategy'],
                customer_campaign['parameter'],
                customer_campaign['mod_percent_max'],
                customer_campaign['mod_percent_min'],
                customer_campaign['max_bid_max'],
                customer_campaign['max_bid_min'],
                customer_campaign['min_bid_max'],
                customer_campaign['min_bid_min'],
                )
         ] for customer_campaign in customers_campaigns
    ]
    try:
        for group_of_campaigns in group_of_campaigns_to_run:
            lookback_thread = AutobidderGroupThread(group_of_campaigns)
    except Exception as ex:
        print(ex)
    else:
        sys.exit(0)
