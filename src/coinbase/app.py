import dataclasses
import json
from dataclasses import dataclass
from time import sleep

import numpy as np
import pandas as pd
from websockets.sync.client import connect

PRODUCT_ID = "BTC-USD"

SUBSCRIBE_JSON = {
    "type": "subscribe",
    "channels": ["level2_batch"],
    "product_ids": [PRODUCT_ID]
}
UNSUBSCRIBE_JSON = {
    "type": "unsubscribe"
}
SUBSCRIBE_MESSAGE = json.dumps(SUBSCRIBE_JSON)
UNSUBSCRIBE_MESSAGE = json.dumps(UNSUBSCRIBE_JSON)


# TODO: does the "current highest" and "current lowest" refer to the values observed from the beginning of the execution, or during the last update?
# TODO make dataclasses private to the CoinbaseFeedAnalyzer
# TODO: work on method names

@dataclass(frozen=True)
class FeedUpdateStats:
    time: pd.Timestamp
    lowest_asks: pd.DataFrame
    highest_bids: pd.DataFrame
    lowest_ask_price: float
    lowest_ask_quantity: float
    highest_bid_price: float
    highest_bid_quantity: float

    @classmethod
    def from_feed(cls, asks_update: pd.DataFrame, bids_update: pd.DataFrame,
                  update_time: pd.Timestamp):
        lowest_asks = asks_update[(asks_update["price_level"] == asks_update["price_level"].min())]
        lowest_asks["time"] = update_time
        highest_bids = bids_update[(bids_update["price_level"] == bids_update["price_level"].max())]
        highest_bids["time"] = update_time

        lowest_ask_price = lowest_asks["price_level"].values[0] if not lowest_asks.empty else np.nan
        lowest_ask_quantity = lowest_asks["quantity"].values[0] if not lowest_asks.empty else np.nan

        highest_bid_price = highest_bids["price_level"].values[0] if not highest_bids.empty else np.nan
        highest_bid_quantity = highest_bids["quantity"].values[0] if not highest_bids.empty else np.nan

        return cls(update_time, lowest_asks, highest_bids, lowest_ask_price, lowest_ask_quantity,
                   highest_bid_price, highest_bid_quantity)


@dataclass(frozen=True)
class FeedState:
    time: pd.Timestamp
    lowest_asks: pd.DataFrame
    highest_bids: pd.DataFrame
    lowest_ask_price: float
    lowest_ask_quantity: float
    highest_bid_price: float
    highest_bid_quantity: float

    @classmethod
    def from_update_stats(cls, stats: FeedUpdateStats) -> "FeedState":
        return cls(stats.time, stats.lowest_asks, stats.highest_bids, stats.lowest_ask_price,
                   stats.lowest_ask_quantity, stats.highest_bid_price,
                   stats.highest_bid_quantity)

    def drop_history(self, before: pd.Timestamp) -> "FeedState":
        # TODO: does pandas know that it is sorted?
        return dataclasses.replace(
            self,
            lowest_asks=self.lowest_asks[self.lowest_asks["time"] > before],
            highest_bids=self.highest_bids[self.highest_bids["time"] > before]
        )

    def update(self, stats: FeedUpdateStats) -> "FeedState":
        lowest_asks = pd.concat([self.lowest_asks, stats.lowest_asks])
        highest_bids = pd.concat([self.highest_bids, stats.highest_bids])

        if stats.lowest_ask_price < self.lowest_ask_price:
            lowest_ask_price = stats.lowest_ask_price
            lowest_ask_quantity = stats.lowest_ask_quantity
        else:
            lowest_ask_price = self.lowest_ask_price
            lowest_ask_quantity = self.lowest_ask_quantity

        if stats.highest_bid_price > self.highest_bid_price:
            highest_bid_price = stats.highest_bid_price
            highest_bid_quantity = stats.highest_bid_quantity
        else:
            highest_bid_price = self.highest_bid_price
            highest_bid_quantity = self.highest_bid_quantity
        return FeedState(
            stats.time,
            lowest_asks,
            highest_bids,
            lowest_ask_price,
            lowest_ask_quantity,
            highest_bid_price,
            highest_bid_quantity
        )

    @property
    def highest_bid_ask_diff(self) -> float:
        return self.highest_bid_price - self.lowest_ask_price

    def __str__(self):
        return f"State for {self.time}:\n" \
               f"  Highest bid: price - {self.highest_bid_price}, quantity - {self.highest_bid_quantity}\n" \
               f"  Lowest ask: price - {self.lowest_ask_price}, quantity - {self.lowest_ask_quantity}\n" \
               f"  Biggest ask and bid price difference: price - {self.highest_bid_ask_diff}."


class CoinbaseFeedAnalyzer:
    DEFAULT_AGGREGATION_WINDOWS_MINUTES = [1, 5, 15]

    def __init__(self, snapshot: dict, product_id: str, aggregation_windows_minutes=None):
        if aggregation_windows_minutes is None:
            aggregation_windows_minutes = self.DEFAULT_AGGREGATION_WINDOWS_MINUTES
            aggregation_windows_minutes.sort()
        self.aggregation_windows = [pd.Timedelta(minutes=m) for m in aggregation_windows_minutes]
        self.product_id = product_id

        snapshot_asks = pd.DataFrame(snapshot["asks"], columns=["price_level", "quantity"], dtype=float)
        snapshot_bids = pd.DataFrame(snapshot["bids"], columns=["price_level", "quantity"], dtype=float)
        # TODO: time from the response does not reflect that 5 seconds passed
        snapshot_time = pd.to_datetime(snapshot["time"])
        feed_update_stats = FeedUpdateStats.from_feed(snapshot_asks, snapshot_bids, snapshot_time)
        self.feed_state = FeedState.from_update_stats(feed_update_stats)
        self.mid_prices = self.__calc_mid_prices()

    def __calc_mid_prices(self):
        # TODO: move to somewhere?
        mid_prices = {}
        for window in self.aggregation_windows:
            window_lowest_asks = self.feed_state.lowest_asks[
                self.feed_state.lowest_asks["time"] > self.feed_state.time - window]
            window_highest_bids = self.feed_state.highest_bids[
                self.feed_state.highest_bids["time"] > self.feed_state.time - window]
            window_lowest_ask_price = window_lowest_asks["price_level"].min()
            window_highest_bid_price = window_highest_bids["price_level"].max()
            mid_price = (window_lowest_ask_price + window_highest_bid_price) / 2
            mid_prices[f"{window.seconds / 60} minute(s)"] = mid_price
        return mid_prices

    def update(self, update: dict):
        update_df = pd.DataFrame(update["changes"], columns=["side", "price_level", "quantity"])
        update_time = pd.to_datetime(update["time"])
        update_asks = update_df[update_df["side"] == "sell"].drop(columns="side").astype(float)
        update_bids = update_df[update_df["side"] == "buy"].drop(columns="side").astype(float)
        feed_update_stats = FeedUpdateStats.from_feed(update_asks, update_bids, update_time)
        self.feed_state = self.feed_state.drop_history(update_time - self.aggregation_windows[-1])
        self.feed_state = self.feed_state.update(feed_update_stats)
        self.mid_prices = self.__calc_mid_prices()

    def print_stats(self):
        print(f"Product {self.product_id}. {self.feed_state}")
        print(f"Mid prices: {self.mid_prices}")


with connect("wss://ws-feed.exchange.coinbase.com") as websocket:
    websocket.send(SUBSCRIBE_MESSAGE)
    subscription_confirmation = json.loads(websocket.recv())
    if subscription_confirmation["type"] == "error":
        # TODO: add logging/printing
        raise Exception(f"Subscription error: {subscription_confirmation}")
    else:
        print("Subscription confirmed.")
    try:
        feed_snapshot = json.loads(websocket.recv())
        analyzer = CoinbaseFeedAnalyzer(feed_snapshot, PRODUCT_ID)
        analyzer.print_stats()
        while True:
            sleep(5)
            update = json.loads(websocket.recv())
            analyzer.update(update)
            analyzer.print_stats()
    finally:
        # TODO: unsubscribe doesn't return expected response, but simply sends the next update
        websocket.send(UNSUBSCRIBE_MESSAGE)
        # unsubscribe_confirmation = json.loads(websocket.recv())
        # print(f"Unsubscription confirmation {unsubscribe_confirmation}")
