import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import websocket
from sortedcontainers import SortedDict

SOURCE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
PRINT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PRINT_FLOAT_ACCURACY = 8


@dataclass(frozen=True)
class Operation:
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
    current_highest_bid: Operation
    current_lowest_ask: Operation
    max_ask_bid_diff: BidAskDiff
    mid_prices: dict[int, float]


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


class OrderBookApp:
    def __init__(self, product_id: str):
        self.product_id = product_id
        self.websocket = None
        self.order_book = None
        self.websocket_thread = None

    def __enter__(self):
        self.websocket = websocket.WebSocketApp(ws_url,
                                                on_open=self.__on_open,
                                                on_message=self.__on_message,
                                                on_error=self.__on_error,
                                                on_close=self.__on_close)
        self.websocket_thread = threading.Thread(target=self.websocket.run_forever)
        self.websocket_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # TODO: add unsubscribe message
        # TODO: not sure if this implementation is correct
        # TODO: send exit message to the websocket
        self.websocket.close()
        self.websocket_thread.join()

    def print_stats(self):
        def __format_datetime_for_print(dt: datetime) -> str:
            return dt.strftime(PRINT_DATETIME_FORMAT)

        def __format_float_for_print(f: float) -> str:
            return f"{f:.{PRINT_FLOAT_ACCURACY}f}"

        current_local_datetime = datetime.now()
        print(f"Order book stats for {self.product_id} at {__format_datetime_for_print(current_local_datetime)}:")
        stats = self.order_book.get_stats()
        print(
            f"  Highest bid: price - {__format_float_for_print(stats.current_highest_bid.price_level)}, "
            f"quantity - {__format_float_for_print(stats.current_highest_bid.quantity)}"
        )
        print(
            f"  Lowest ask: price - {__format_float_for_print(stats.current_lowest_ask.price_level)}, "
            f"quantity - {__format_float_for_print(stats.current_lowest_ask.quantity)}"
        )
        print(
            f"  The biggest difference in price between the highest bid and the lowest ask we have seen so far is "
            f"{__format_float_for_print(stats.max_ask_bid_diff.diff)}, "
            f"observed at {__format_datetime_for_print(stats.max_ask_bid_diff.observed_at)}."
        )
        print(
            "  Mid prices for the defined aggregation windows: " +
            (", ".join([
                f"{seconds / 60} minute(s) - {__format_float_for_print(mid_price)}"
                for seconds, mid_price
                in stats.mid_prices.items()
            ]))
        )
        print()

    def __on_open(self, ws):
        subscribe_message = {
            "type": "subscribe",
            "channels": [{"name": "level2_batch", "product_ids": [self.product_id]}]
        }
        ws.send(json.dumps(subscribe_message))

    def __on_message(self, _, message):
        data = json.loads(message)
        if data['type'] == 'snapshot':
            self.order_book = OrderBook(data)
        elif data['type'] == 'l2update':
            self.order_book.update(data)
        else:
            raise ValueError(f"Unknown message is received: {message}")

    def __on_error(self, ws, error):
        print(f"Error: {error}")

    def __on_close(self, ws, close_status_code, close_msg):
        print("Closed")


if __name__ == "__main__":
    ws_url = "wss://ws-feed.pro.coinbase.com"
    product_id = "BTC-USD"
    with OrderBookApp(product_id) as app:
        while True:
            time.sleep(5)
            app.print_stats()
