from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Order:
    price_level: float
    quantity: float


@dataclass(frozen=True)
class BidAskDiff:
    highest_bid_price_level: float
    lowest_ask_price_level: float
    observed_at: datetime

    @property
    def diff(self):
        return self.lowest_ask_price_level - self.highest_bid_price_level


@dataclass(frozen=True)
class OrderBookStats:
    current_highest_bid: Order
    current_lowest_ask: Order
    max_ask_bid_diff: BidAskDiff
    forecasted_mid_price: float
    mid_prices: dict[int, float]
    forecast_errors: dict[int, float]
