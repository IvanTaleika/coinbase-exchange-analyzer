from datetime import datetime

import pandas as pd
from sortedcontainers import SortedDict

from coinbase.model import OrderBookStats, BidAskDiff, Operation

SOURCE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class OrderBook:
    DEFAULT_AGGREGATION_WINDOWS_MINUTES = [60, 60 * 5, 60 * 15]

    def __init__(self, snapshot: dict, aggregation_windows_sec=None):
        if aggregation_windows_sec is None:
            aggregation_windows_sec = self.DEFAULT_AGGREGATION_WINDOWS_MINUTES
            aggregation_windows_sec.sort()
        self._aggregation_windows = [pd.Timedelta(seconds=m) for m in aggregation_windows_sec]
        self._bids: SortedDict[float, float] = SortedDict(lambda x: -x)
        self._asks: SortedDict[float, float] = SortedDict()
        self._mid_prices = pd.Series()

        snapshot_time = self.__parse_request_time(snapshot['time'])

        for price_level_s, quantity_s in snapshot['bids']:
            self.__insert_record('buy', float(price_level_s), float(quantity_s))

        for price_level_s, quantity_s in snapshot['asks']:
            self.__insert_record('sell', float(price_level_s), float(quantity_s))

        self._max_ask_bid_diff = self.__calc_ask_bid_diff(snapshot_time)
        self.__update_mid_prices(self._max_ask_bid_diff)
        self._last_update = snapshot_time

    def update(self, update: dict):
        update_time = self.__parse_request_time(update['time'])

        for side, price_level_s, quantity_s in update['changes']:
            self.__insert_record(side, float(price_level_s), float(quantity_s))

        new_operations_diff = self.__calc_ask_bid_diff(update_time)
        self._max_ask_bid_diff = (
            new_operations_diff if new_operations_diff.diff > self._max_ask_bid_diff.diff else self._max_ask_bid_diff
        )
        self.__update_mid_prices(new_operations_diff)
        self._last_update = update_time

    def take_snapshot(self) -> (SortedDict[float], SortedDict[float]):
        bids = self._bids.copy()
        asks = self._asks.copy()
        return bids, asks

    def get_stats(self):

        mid_price_stats = {}
        for window in self._aggregation_windows:
            window_mid_prices = self._mid_prices[self._last_update - window:]
            avg_mid_price = window_mid_prices.mean()
            mid_price_stats[window.seconds] = avg_mid_price

        return OrderBookStats(
            self.__get_first_record('buy'),
            self.__get_first_record('sell'),
            self._max_ask_bid_diff,
            mid_price_stats
        )

    def __update_mid_prices(self, current_diff: BidAskDiff):
        cut_off_time = current_diff.observed_at - self._aggregation_windows[-1]
        mid_price = (current_diff.highest_bid_price_level + current_diff.lowest_ask_price_level) / 2
        new_mid_prices = pd.concat([self._mid_prices, pd.Series([mid_price], index=[current_diff.observed_at])])
        new_mid_prices = new_mid_prices[cut_off_time:]
        self._mid_prices = new_mid_prices

    def __parse_request_time(self, request_time: str) -> datetime:
        return datetime.strptime(request_time, SOURCE_DATETIME_FORMAT)

    def __calc_ask_bid_diff(self, tm: datetime) -> BidAskDiff:
        return BidAskDiff(
            self.__get_first_record('buy').price_level,
            self.__get_first_record('sell').price_level,
            tm
        )

    def __get_book(self, side):
        return self._bids if side == 'buy' else self._asks

    def __get_first_record(self, side) -> Operation:
        # TODO: handle empty dict that throws "IndexError: list index out of range". Return np.nan?
        book = self.__get_book(side)
        return Operation(*book.peekitem(index=0))

    def __insert_record(self, side: str, price_level: float, quantity: float):
        book = self.__get_book(side)
        if quantity > 0:
            book[price_level] = quantity
        elif price_level in book:
            del book[price_level]
