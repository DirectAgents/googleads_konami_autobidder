from typing import Tuple

import numpy as np
import pandas as pd
from google.protobuf.json_format import MessageToDict
from google.ads.googleads.client import GoogleAdsClient
from utils.config import get_inputs_config, get_google_ads_credentials_file_path


config = get_inputs_config()

customer_id = str(config['CUSTOMER_ID'])
campaign_id = str(config['CAMPAIGN_ID'])

credentials_yaml_path = get_google_ads_credentials_file_path()
gads_client = GoogleAdsClient.load_from_storage(path=credentials_yaml_path, version="v10")
gads_service = gads_client.get_service("GoogleAdsService")

# TODAY PERFORMANCE
TODAY_PERFORMANCE_QUERY = f"""
    SELECT
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.all_conversions_from_interactions_rate,
        metrics.cost_per_all_conversions,
        metrics.search_impression_share,
        segments.hour,
        segments.date
    FROM campaign
    WHERE campaign.id = {campaign_id}
"""

# ADGROUP_PERFORMANCE_QUERY
ADGROUP_PERFORMANCE_QUERY = f"""
    SELECT
        ad_group.name,
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.all_conversions_from_interactions_rate,
        metrics.cost_per_all_conversions,
        metrics.search_impression_share,
        segments.hour,
        segments.date
    FROM ad_group
    WHERE campaign.id = {campaign_id}
"""


def build_report_query(report_type: str, start_date: str, end_date: str, adgroup_names: Tuple[str] = None) -> str:
    report_query = TODAY_PERFORMANCE_QUERY if report_type == 'TODAY_PERFORMANCE_REPORT' else ADGROUP_PERFORMANCE_QUERY
    if adgroup_names:
        if len(adgroup_names) == 1:
            report_query += f" AND ad_group.name = '{adgroup_names[0]}'"
        else:
            report_query += f" AND ad_group.name IN {adgroup_names}"

    report_query += f" AND segments.date BETWEEN '{start_date}' AND '{end_date}'"
    return report_query


def format_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    formatted_dataframe = dataframe.rename(columns={
        'campaign.id': 'campaign_id',
        'campaign.name': 'campaign_name',
        'metrics.impressions': 'impressions',
        'metrics.clicks': 'clicks',
        'metrics.allConversions': 'conversions',
        'metrics.allConversionsValue': 'conversions_value',
        'metrics.allConversionsFromInteractionsRate': 'conversion_rate',
        'metrics.costPerAllConversions': 'cost_per_conversion',
        'metrics.searchImpressionShare': 'search_impression_share',
        'segments.date': 'day',
        'segments.hour': 'hour_of_day',
        'segments.dayOfWeek': 'day_of_week',
        'campaignBudget.amountMicros': 'amount_micros',
        'metrics.costMicros': 'cost_micros',
        'campaignBudget.id': 'budget_id',
        'segments.conversionLagBucket': 'conversion_lag_bucket',
        'adGroup.name': 'adgroup_name'
    }).astype({
        'impressions': 'int',
        'cost_per_conversion': 'float',
        'cost_micros': 'float'
    })
    try:
        formatted_dataframe.drop('campaign.resourceName', inplace=True, axis=1)
    except KeyError:
        pass

    try:
        formatted_dataframe.drop('campaignBudget.resourceName', inplace=True, axis=1)
    except KeyError:
        pass

    try:
        formatted_dataframe.drop('adGroup.resourceName', inplace=True, axis=1)
    except KeyError:
        pass

    try:
        formatted_dataframe.drop('segments.hour', inplace=True, axis=1)
    except KeyError:
        pass

    return formatted_dataframe


def clean_dataframe(dataframe: pd.DataFrame):
    df_today = dataframe[0:-1]
    try:
        df_today['hour_of_day'] = df_today['hour_of_day'].astype(int)
        df_today.sort_values('hour_of_day', inplace=True)
    except KeyError:
        pass
    try:
        df_today['search_impression_share'] = df_today['search_impression_share'].apply(
            lambda x: np.nan if x == ' --' or x == '< 10%' else x)
    except KeyError:
        pass
    try:
        df_today['cost'] = df_today['cost_micros'].apply(lambda x: x / 1_000_000, 1)
        df_today['cost'] = df_today['cost'].astype(float)
    except KeyError:
        pass
    try:
        df_today['budget'] = df_today['amount_micros'].apply(lambda x: x / 1_000_000, 1)
    except KeyError:
        pass
    try:
        df_today['roas'] = df_today.apply(
            lambda x: 0 if x['cost'] == 0 else x['conversions_value'] / x['cost'], 1
        )
    except KeyError:
        pass
    return df_today


def get_gads_report(report_type: str, start_date, end_date, adgroup_names=None) -> pd.DataFrame:
    report_query = build_report_query(report_type, start_date, end_date, adgroup_names)
    response = gads_service.search(customer_id=customer_id, query=report_query)
    response_to_dict = MessageToDict(response)
    response_to_dataframe = pd.json_normalize(response_to_dict, record_path=['results'])
    formatted_dataframe = format_dataframe(response_to_dataframe)
    cleaned_dataframe = clean_dataframe(formatted_dataframe)
    return cleaned_dataframe
