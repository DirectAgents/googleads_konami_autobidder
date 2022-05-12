from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers
from utils.config import get_inputs_config

inputs_config = get_inputs_config()
CAMPAIGN_ID = str(inputs_config['CAMPAIGN_ID'])
CUSTOMER_ID = str(inputs_config['CUSTOMER_ID'])
# CUSTOMER_ID = str(7167910385)
# CAMPAIGN_ID = str(9440617334)


gads_client = GoogleAdsClient.load_from_storage(version="v10")


def get_bid_strategy_id(client):
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT bidding_strategy.id
        FROM campaign WHERE campaign.id = "{CAMPAIGN_ID}"
        """

    # Issues a search request using streaming.
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = CUSTOMER_ID
    search_request.query = query
    stream = ga_service.search_stream(search_request)
    for batch in stream:
        for row in batch.results:
            bidding_strategy = row.bidding_strategy
            return bidding_strategy.id


def tcpa_max(client, max_bid_value):
    bidding_strategy_service = client.get_service("BiddingStrategyService")
    bidding_strategy_operation = client.get_type("BiddingStrategyOperation")
    bidding_strategy = bidding_strategy_operation.update
    bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
        CUSTOMER_ID, get_bid_strategy_id(client)
    )

    bidding_strategy.target_cpa.cpc_bid_ceiling_micros = max_bid_value
    field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
    client.copy_from(bidding_strategy_operation.update_mask, field_mask)

    bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
        customer_id=CUSTOMER_ID, operations=[bidding_strategy_operation]
    )
    if bidding_strategy_response:
        print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
        return True
    else:
        return False


def tcpa_both(client, max_bid_value, min_bid_value):
    bidding_strategy_service = client.get_service("BiddingStrategyService")
    bidding_strategy_operation = client.get_type("BiddingStrategyOperation")
    bidding_strategy = bidding_strategy_operation.update
    bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
        CUSTOMER_ID, get_bid_strategy_id(client)
    )

    bidding_strategy.target_cpa.cpc_bid_ceiling_micros = max_bid_value
    bidding_strategy.target_cpa.cpc_bid_floor_micros = min_bid_value
    field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
    client.copy_from(bidding_strategy_operation.update_mask, field_mask)

    bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
        customer_id=CUSTOMER_ID, operations=[bidding_strategy_operation]
    )
    if bidding_strategy_response:
        print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
        return True
    else:
        return False


if __name__ == '__main__':
    tcpa_both(gads_client, 250000, 200000)


def troas_max(client, max_bid_value):
    bidding_strategy_service = client.get_service("BiddingStrategyService")
    bidding_strategy_operation = client.get_type("BiddingStrategyOperation")
    bidding_strategy = bidding_strategy_operation.update
    bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
        CUSTOMER_ID, get_bid_strategy_id(client)
    )

    bidding_strategy.target_roas.cpc_bid_ceiling_micros = max_bid_value
    field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
    client.copy_from(bidding_strategy_operation.update_mask, field_mask)

    bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
        customer_id=CUSTOMER_ID, operations=[bidding_strategy_operation]
    )
    if bidding_strategy_response:
        print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
        return True
    else:
        return False


def troas_both(client, max_bid_value, min_bid_value):
    bidding_strategy_service = client.get_service("BiddingStrategyService")
    bidding_strategy_operation = client.get_type("BiddingStrategyOperation")
    bidding_strategy = bidding_strategy_operation.update
    bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
        CUSTOMER_ID, get_bid_strategy_id(client)
    )

    bidding_strategy.target_roas.cpc_bid_ceiling_micros = max_bid_value
    bidding_strategy.target_roas.cpc_bid_floor_micros = min_bid_value
    field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
    client.copy_from(bidding_strategy_operation.update_mask, field_mask)

    bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
        customer_id=CUSTOMER_ID, operations=[bidding_strategy_operation]
    )
    if bidding_strategy_response:
        print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
        return True
    else:
        return False
