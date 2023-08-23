import dataclasses
import datetime
import unittest

from sortedcontainers import SortedDict

from coinbase.model import OrderBookStats, BidAskDiff, Operation
from coinbase.order_book import OrderBook


# Some of the tests are not implemented, because the functionality isn't essential for the POC project.
# The tests are outlined because they are important for the production code.
# TODO: think about proper package structure for tests
class OrderBookTest(unittest.TestCase):

    def init_test_order_book(self):
        # no compaction by default
        snapshot = {
            'type': 'snapshot',
            'product_id': 'BTC-USD',
            'asks': [
                [
                    '100.0',
                    '10.1'
                ],
                [
                    '20.1',
                    '20.0'
                ],
                [
                    '30.5',
                    '0.0'
                ],
                [
                    '400.0',
                    '400.0'
                ],
            ],
            'bids': [
                [
                    '10.0',
                    '1.1'
                ],
                [
                    '2.1',
                    '2.0'
                ],
                [
                    '3.5',
                    '0.0'
                ],
                [
                    '14.0',
                    '14.0'
                ],
            ],
            'time': '2023-01-01T00:00:00.00000Z'
        }

        expected_bids = SortedDict(lambda x: -x, {
            (14.0, 14.0),
            (10.0, 1.1),
            (2.1, 2.0),
        })

        expected_asks = SortedDict({
            (20.1, 20.0),
            (100.0, 10.1),
            (400.0, 400.0),
        })
        mid_price = (20.1 + 14.0) / 2

        expected_stats = OrderBookStats(
            Operation(14.0, 14.0),
            Operation(20.1, 20.0),
            BidAskDiff(14.0, 20.1, datetime.datetime(2023, 1, 1, 0, 0, 0)),
            {60: mid_price, 5 * 60: mid_price, 15 * 60: mid_price}
        )

        order_book = OrderBook(snapshot)
        return order_book, expected_bids, expected_asks, expected_stats

    def test_take_snapshot_object_cant_be_used_to_update_order_book(self):
        pass

    def test_order_book_constructor_creates_ordered_asks_and_bids_data_structures(self):
        order_book, expected_bids, expected_asks, expected_stats = self.init_test_order_book()
        actual_bids, actual_asks = order_book.take_snapshot()
        actual_stats = order_book.get_stats()

        self.assertEqual(expected_bids, actual_bids)
        self.assertEqual(expected_asks, actual_asks)
        self.assertEqual(expected_stats, actual_stats)

    def test_update_updates_asks_bids_and_stats(self):
        order_book, default_bids, default_asks, default_stats = self.init_test_order_book()
        # All windows have the same mid_price
        default_mid_price = default_stats.mid_prices[60]

        update = {
            'type': 'l2update',
            'product_id': 'BTC-USD',
            'changes': [],
            'time': '2023-01-01T00:00:01.00000Z'
        }
        order_book.update(update)

        expected_bids = default_bids
        expected_asks = default_asks
        expected_stats = default_stats

        update_0_mid_price = default_mid_price

        actual_bids, actual_asks = order_book.take_snapshot()
        actual_stats = order_book.get_stats()

        self.assertEqual(expected_bids, actual_bids)
        self.assertEqual(expected_asks, actual_asks)
        self.assertEqual(expected_stats, actual_stats)

        update = {
            'type': 'l2update',
            'product_id': 'BTC-USD',
            'changes': [
                [
                    'buy',
                    '10.0',
                    '0.0'
                ],
                [
                    'buy',
                    '2.1',
                    '20.0'
                ],
                [
                    'buy',
                    '15.0',
                    '5.0'
                ],
            ],
            'time': '2023-01-01T00:01:01.00000Z'
        }
        order_book.update(update)

        expected_bids = SortedDict(lambda x: -x, {
            (15.0, 5.0),
            (14.0, 14.0),
            (2.1, 20.0),
        })

        update_1_mid_price = (15.0 + 20.1) / 2
        update_1_mid_prices = {
            60: (update_1_mid_price + update_0_mid_price) / 2,
            5 * 60: (update_1_mid_price + update_0_mid_price + default_mid_price) / 3,
            15 * 60: (update_1_mid_price + update_0_mid_price + default_mid_price) / 3
        }
        expected_stats = dataclasses.replace(
            expected_stats,
            current_highest_bid=Operation(15.0, 5.0),
            mid_prices=update_1_mid_prices
        )

        actual_bids, actual_asks = order_book.take_snapshot()
        actual_stats = order_book.get_stats()

        self.assertEqual(expected_bids, actual_bids)
        self.assertEqual(expected_asks, actual_asks)
        self.assertEqual(expected_stats, actual_stats)

        update = {
            'type': 'l2update',
            'product_id': 'BTC-USD',
            'changes': [
                [
                    'sell',
                    '20.1',
                    '0.0'
                ],
                [
                    'sell',
                    '100.0',
                    '5.0'
                ],
                [
                    'sell',
                    '40.6',
                    '0.6'
                ],
            ],
            'time': '2023-01-01T00:05:01.00000Z'
        }
        order_book.update(update)

        expected_asks = SortedDict({
            (40.6, 0.6),
            (100.0, 5.0),
            (400.0, 400.0),
        })

        update_2_mid_price = (15.0 + 40.6) / 2
        update_2_mid_prices = {
            60: update_2_mid_price,
            5 * 60: (update_2_mid_price + update_1_mid_price + update_0_mid_price) / 3,
            15 * 60: (update_2_mid_price + update_1_mid_price + update_0_mid_price + default_mid_price) / 4
        }
        expected_stats = OrderBookStats(
            Operation(15.0, 5.0),
            Operation(40.6, 0.6),
            BidAskDiff(15.0, 40.6, datetime.datetime(2023, 1, 1, 0, 5, 1)),
            update_2_mid_prices
        )

        actual_bids, actual_asks = order_book.take_snapshot()
        actual_stats = order_book.get_stats()

        self.assertEqual(expected_bids, actual_bids)
        self.assertEqual(expected_asks, actual_asks)
        self.assertEqual(expected_stats, actual_stats)

        update = {
            'type': 'l2update',
            'product_id': 'BTC-USD',
            'changes': [
                [
                    'sell',
                    '40.6',
                    '0.7'
                ],
                [
                    'buy',
                    '14.0',
                    '0.0'
                ],
                [
                    'sell',
                    '40.9',
                    '90.0'
                ],
                [
                    'buy',
                    '2.1',
                    '100.0'
                ],
            ],
            'time': '2023-01-01T00:15:01.00000Z'
        }
        order_book.update(update)

        expected_bids = SortedDict(lambda x: -x, {
            (15.0, 5.0),
            (2.1, 100.0),
        })

        expected_asks = SortedDict({
            (40.6, 0.7),
            (40.9, 90.0),
            (100.0, 5.0),
            (400.0, 400.0),
        })

        update_3_mid_price = (15.0 + 40.6) / 2
        update_3_mid_prices = {
            60: update_3_mid_price,
            5 * 60: update_3_mid_price,
            15 * 60: (update_3_mid_price + update_2_mid_price + update_1_mid_price + update_0_mid_price) / 4
        }
        expected_stats = dataclasses.replace(
            expected_stats,
            current_lowest_ask=Operation(40.6, 0.7),
            mid_prices=update_3_mid_prices
        )

        actual_bids, actual_asks = order_book.take_snapshot()
        actual_stats = order_book.get_stats()

        self.assertEqual(expected_bids, actual_bids)
        self.assertEqual(expected_asks, actual_asks)
        self.assertEqual(expected_stats, actual_stats)
