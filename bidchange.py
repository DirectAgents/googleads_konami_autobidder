from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers


def main(client, customer_id, bidding_strategy_id, cpc_bid_ceiling_micros):
    bidding_strategy_service = client.get_service("BiddingStrategyService")
    bidding_strategy_operation = client.get_type("BiddingStrategyOperation")

    bidding_strategy = bidding_strategy_operation.update
    bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
        customer_id, bidding_strategy_id
    )

    bidding_strategy.target_cpa.cpc_bid_ceiling_micros = cpc_bid_ceiling_micros
    field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
    client.copy_from(bidding_strategy_operation.update_mask, field_mask)

    bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
        customer_id=customer_id, operations=[bidding_strategy_operation]
    )
    print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")


if __name__ == "__main__":
    gads_client = GoogleAdsClient.load_from_storage(version="v10")
    main(gads_client, "9916111035", "7053362983", 2500000)
