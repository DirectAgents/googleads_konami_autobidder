from google.api_core import protobuf_helpers
from utils.build_gads_client import get_google_ads_client


class GoogleAdsBidChangeExecutor:
    def __init__(self, customer_id: str, campaign_id: str):
        self.client = get_google_ads_client()
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.bid_strategy_id = self.get_bid_strategy_id()

    def get_bidding_service_and_operation(self) -> tuple:
        return (
            self.client.get_service("BiddingStrategyService"),
            self.client.get_type("BiddingStrategyOperation")
        )

    def get_bid_strategy_id(self) -> str:
        ga_service = self.client.get_service("GoogleAdsService")
        query = f"""
            SELECT bidding_strategy.id
            FROM campaign WHERE campaign.id = "{self.campaign_id}"
            """

        # Issues a search request using streaming.
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        stream = ga_service.search_stream(search_request)
        for batch in stream:
            for row in batch.results:
                bidding_strategy = row.bidding_strategy
                return bidding_strategy.id

    def tcpa_max(self, max_bid_value):
        bidding_strategy_service, bidding_strategy_operation = self.get_bidding_service_and_operation()
        bidding_strategy = bidding_strategy_operation.update
        bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
            self.customer_id, self.bid_strategy_id
        )

        bidding_strategy.target_cpa.cpc_bid_ceiling_micros = max_bid_value
        field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
        self.client.copy_from(bidding_strategy_operation.update_mask, field_mask)

        bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
            customer_id=self.customer_id, operations=[bidding_strategy_operation]
        )
        if bidding_strategy_response:
            print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
            return True
        else:
            return False

    def tcpa_both(self, max_bid_value, min_bid_value):
        bidding_strategy_service, bidding_strategy_operation = self.get_bidding_service_and_operation()
        bidding_strategy = bidding_strategy_operation.update
        bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
            self.customer_id, self.bid_strategy_id
        )

        bidding_strategy.target_cpa.cpc_bid_ceiling_micros = max_bid_value
        bidding_strategy.target_cpa.cpc_bid_floor_micros = min_bid_value
        field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
        self.client.copy_from(bidding_strategy_operation.update_mask, field_mask)

        bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
            customer_id=self.customer_id, operations=[bidding_strategy_operation]
        )
        if bidding_strategy_response:
            print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
            return True
        else:
            return False

    def troas_max(self, max_bid_value):
        bidding_strategy_service, bidding_strategy_operation = self.get_bidding_service_and_operation()
        bidding_strategy = bidding_strategy_operation.update
        bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
            self.customer_id, self.bid_strategy_id
        )

        bidding_strategy.target_roas.cpc_bid_ceiling_micros = max_bid_value
        field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
        self.client.copy_from(bidding_strategy_operation.update_mask, field_mask)

        bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
            customer_id=self.customer_id, operations=[bidding_strategy_operation]
        )
        if bidding_strategy_response:
            print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
            return True
        else:
            return False

    def troas_both(self, max_bid_value, min_bid_value):
        bidding_strategy_service, bidding_strategy_operation = self.get_bidding_service_and_operation()
        bidding_strategy = bidding_strategy_operation.update
        bidding_strategy.resource_name = bidding_strategy_service.bidding_strategy_path(
            self.customer_id, self.bid_strategy_id
        )

        bidding_strategy.target_roas.cpc_bid_ceiling_micros = max_bid_value
        bidding_strategy.target_roas.cpc_bid_floor_micros = min_bid_value
        field_mask = protobuf_helpers.field_mask(None, bidding_strategy)
        self.client.copy_from(bidding_strategy_operation.update_mask, field_mask)

        bidding_strategy_response = bidding_strategy_service.mutate_bidding_strategies(
            customer_id=self.customer_id, operations=[bidding_strategy_operation]
        )
        if bidding_strategy_response:
            print(f"Updated Bidding Strategy {bidding_strategy_response.results[0].resource_name}.")
            return True
        else:
            return False

    def execute(self, bid_strategy: str, parameter: str, max_bid_value: int, min_bid_value: int):
        if bid_strategy == 'tcpa' and parameter == 'max':
            self.tcpa_max(max_bid_value)
        elif bid_strategy == 'tcpa' and parameter == 'both':
            self.tcpa_both(max_bid_value, min_bid_value)
        elif bid_strategy == 'troas' and parameter == 'max':
            self.troas_max(max_bid_value)
        else:
            self.troas_both(max_bid_value, min_bid_value)
