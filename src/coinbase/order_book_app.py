import json
import logging
import threading
from datetime import datetime

import numpy as np
import websocket

from coinbase.order_book import OrderBook
from coinbase.app_logging import print_cmd

PRINT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PRINT_FLOAT_ACCURACY = 8
__logger = logging.getLogger(__name__)


class OrderBookApp:
    def __init__(self, ws_url, product_id: str):
        self.ws_url = ws_url
        self.product_id = product_id
        self.websocket = None
        self.order_book = None
        self.websocket_thread = None

    def __enter__(self):
        self.websocket = websocket.WebSocketApp(self.ws_url,
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
            return f"{f:.{PRINT_FLOAT_ACCURACY}f}" if f != np.nan else "not yet available"

        current_local_datetime = datetime.now()
        print_cmd(f"Order book stats for {self.product_id} at {__format_datetime_for_print(current_local_datetime)}:")
        stats = self.order_book.get_stats()
        print_cmd(
            f"  1.1. Highest bid: price - {__format_float_for_print(stats.current_highest_bid.price_level)}, "
            f"quantity - {__format_float_for_print(stats.current_highest_bid.quantity)}"
        )
        print_cmd(
            f"  1.2. Lowest ask: price - {__format_float_for_print(stats.current_lowest_ask.price_level)}, "
            f"quantity - {__format_float_for_print(stats.current_lowest_ask.quantity)}"
        )
        print_cmd(
            f"  2. The biggest difference in price between the highest bid and the lowest ask we have seen so far is "
            f"{__format_float_for_print(stats.max_ask_bid_diff.diff)}, "
            f"observed at {__format_datetime_for_print(stats.max_ask_bid_diff.observed_at)}"
        )
        print_cmd(
            "  3. Mid prices for the defined aggregation windows: " +
            (", ".join([
                f"{seconds / 60} minute(s) - {__format_float_for_print(mid_price)}"
                for seconds, mid_price
                in stats.mid_prices.items()
            ]))
        )
        print_cmd(
            f"  4. Forecasted mid price in 60 seconds - {__format_float_for_print(stats.forecasted_mid_price)}\n"
        )


    def __on_open(self, ws):
        logging.info("Initializing the websocket connection")
        subscribe_message = {
            "type": "subscribe",
            "channels": [{"name": "level2_batch", "product_ids": [self.product_id]}]
        }
        ws.send(json.dumps(subscribe_message))

    def __on_message(self, _, message):
        # TODO: I'm receiving 53 updates and then nothing. Am I hitting the rate limit?
        data = json.loads(message)
        message_type = data['type']
        if message_type == 'snapshot':
            logging.debug(f"Received level 2 snapshot taken at {data['time']}")
            self.order_book = OrderBook(data)
        elif message_type == 'l2update':
            logging.debug(f"Received update taken at {data['time']}")
            self.order_book.update(data)
        elif message_type == 'subscriptions':
            print_cmd(f"Subscribed to level 2 channel for {self.product_id} product")
        else:
            logging.warning(f"Received unexpected message type: {message_type}. Message is ignored.")

    def __on_error(self, ws, error):
        logging.error(f"Error: {error}")

    def __on_close(self, ws, close_status_code, close_msg):
        print_cmd("Level 2 channel closed")
