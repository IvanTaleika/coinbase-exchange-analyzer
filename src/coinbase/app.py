import json
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import PriorityQueue

import pandas as pd
import websocket


@dataclass(frozen=True)
class Operation(ABC):
    price_level: float
    quantity: float

    @property
    @abstractmethod
    def priority(self):
        pass

    def __lt__(self, other):
        return self.priority < other.priority


class Bid(Operation):
    @property
    def priority(self):
        return -self.price_level


class Ask(Operation):
    @property
    def priority(self):
        return self.price_level


@dataclass(frozen=True)
class OperationsDiff:
    bid: Bid
    ask: Ask
    # TODO: which timestamp type to use?
    observed_at: str

    @property
    def diff(self):
        return self.ask.price_level - self.bid.price_level

    def __lt__(self, other):
        return self.diff < other.diff

    def __eq__(self, other):
        return self.diff == other.diff

    def __gt__(self, other):
        return self.diff > other.diff


class OrderBook:
    __ZERO_QUANTITY_THRESHOLD = 0.1

    def __init__(self, snapshot: dict):
        self.bids: PriorityQueue[Bid] = PriorityQueue()
        self.asks: PriorityQueue[Ask] = PriorityQueue()
        self.n_bids_zeros = 0
        self.n_asks_zeros = 0

        snapshot_time = snapshot['time']

        for price_level_s, quantity_s in snapshot['bids']:
            self.__insert_record('buy', float(price_level_s), float(quantity_s))

        for price_level_s, quantity_s in snapshot['asks']:
            self.__insert_record('sell', float(price_level_s), float(quantity_s))

        self.__compact()
        self.operations_diff = OperationsDiff(self.__get_first_record('buy'), self.__get_first_record('sell'), snapshot_time)

    def update(self, update: dict):
        update_time = update['time']

        for side, price_level_s, quantity_s in update['changes']:
            self.__insert_record('buy', float(price_level_s), float(quantity_s))
        self.__compact()

        new_operations_diff = OperationsDiff(self.__get_first_record('buy'), self.__get_first_record('sell'), update_time)
        self.operations_diff = new_operations_diff if new_operations_diff > self.operations_diff else self.operations_diff

    def __get_first_record(self, side) -> Operation:
        if side == 'buy':
            pq = self.bids
        else:
            pq = self.asks
        while not pq.empty():
            record = pq.queue[0]
            if record.quantity > 0:
                return record
            else:
                pq.get()
                self.__change_n_zeros(side, -1)
        else:
            raise ValueError(f"Operation book for '{side}'s is empty")

    def __insert_record(self, side, price_level, quantity):
        if side == 'buy':
            self.bids.put(Bid(price_level, quantity))
        else:
            self.asks.put(Ask(price_level, quantity))

        if quantity == 0:
            self.__change_n_zeros(side, 1)

    def __change_n_zeros(self, side: str, by: int):
        if side == 'buy':
            self.n_bids_zeros += by
        else:
            self.n_asks_zeros += by

    def __compact(self):
        # Algorithms theory describes an IndexPriorityQueue (for example,
        # https://algs4.cs.princeton.edu/24pq/IndexMinPQ.java.html) with O(log(n)) deletion. We can define max
        # precision for the price, multiply it by 10^precision and use it as an index. Then we can delete 0 quantity
        # orders in O(log(n)) time. However, the only implementation for python I was able to find
        # (https://github.com/nvictus/priority-queue-dictionary) isn't widely used. While it can be OK for PoC,
        # I won't use such a solution in production. That is why we keep zeros in the queue and compact it from time
        # to time instead.
        self.n_bids_zeros, self.bids = self.__compact_queue(self.n_bids_zeros, self.bids)
        self.n_asks_zeros, self.asks = self.__compact_queue(self.n_asks_zeros, self.asks)

    # TODO: change to static?
    def __compact_queue(self, n_zeros: int, pq: PriorityQueue[Operation]) -> (int, PriorityQueue[Operation]):
        if n_zeros / pq.qsize() > self.__ZERO_QUANTITY_THRESHOLD:
            new_pq = PriorityQueue()
            # The overall complexity is O(m*log(m)). Additionally, the source queue is already partially sorted,
            # reducing number of swim operations we need to insert a record into the new queue.
            for operation in pq.queue:
                if operation.quantity > 0:
                    new_pq.put(operation)
            return 0, new_pq
        else:
            return n_zeros, pq

    def print_stats(self):
        highest_bid = self.__get_first_record('buy')
        lowest_ask = self.__get_first_record('sell')
        print(f"Highest bid: price - {highest_bid.price_level}, quantity - {highest_bid.quantity}")
        print(f"Lowest ask: price - {lowest_ask.price_level}, quantity - {lowest_ask.quantity}")

        print(f"Biggest difference in price between the highest bid and lowest ask we have seen so far is "
              f"{self.operations_diff.diff}, observed at {self.operations_diff.observed_at}.")


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
        # TODO: not sure if this implementation is correct
        # TODO: send exit message to the websocket
        self.websocket.close()
        self.websocket_thread.join()

    def print_stats(self):
        # TODO: don't use pandas?
        current_time = pd.to_datetime('now')
        print(f"Order book stats for {self.product_id} at {current_time}:")
        self.order_book.print_stats()

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
