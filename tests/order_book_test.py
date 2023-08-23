import dataclasses
import unittest
from queue import PriorityQueue

from coinbase.app import OrderBook, Bid, Ask, OrderBookStats, OperationsDiff
from coinbase.immutable_objects_priority_queue import ImmutableObjectsPriorityQueue


# TODO: think about proper package structure for tests
# TODO: does assertEqual call __eq__ method? Because this does not work for ImmutableObjectsPriorityQueue
# self.assertEqual(expected_bids, actual_bids)
class OrderBookTest(unittest.TestCase):

    # TODO: do we need to somehow mark the assert method?
    # TODO: remove?
    def assert_priority_queues_equal(self, expected: PriorityQueue, actual: PriorityQueue):
        while not expected.empty() and not actual.empty():
            if expected.empty():
                self.fail(f"Expected PQ contains less elements than actual PQ. Actual queue: {actual.queue}")
            self.assertEqual(expected.get(), actual.get())

    def init_test_order_book(self, compaction_threshold=2):
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

        expected_bids = ImmutableObjectsPriorityQueue.from_args(
            Bid(14.0, 14.0),
            Bid(10.0, 1.1),
            Bid(3.5, 0.0),
            Bid(2.1, 2.0),
        )

        expected_asks = ImmutableObjectsPriorityQueue.from_args(
            Ask(20.1, 20.0),
            Ask(30.5, 0.0),
            Ask(100.0, 10.1),
            Ask(400.0, 400.0),
        )

        expected_stats = OrderBookStats(
            Bid(14.0, 14.0),
            Ask(20.1, 20.0),
            OperationsDiff(Bid(14.0, 14.0), Ask(20.1, 20.0), '2023-01-01T00:00:00.00000Z')
        )

        order_book = OrderBook(snapshot, compaction_threshold)
        return order_book, expected_bids, expected_asks, expected_stats

    # Some of the tests are not implemented, because the functionality isn't essential for the POC project.
    # The tests are outlined because they are important for the production code.
    def test_take_snapshot_object_cant_be_used_to_update_order_book(self):
        pass

    def test_take_snapshot_compact_argument_works(self):
        pass

    def test_order_book_constructor_creates_ordered_asks_and_bids_data_structures(self):
        order_book, expected_bids, expected_asks, expected_stats = self.init_test_order_book()
        actual_bids, actual_asks = order_book.take_snapshot(False)
        actual_stats = order_book.get_stats()

        self.assertTrue(expected_bids == actual_bids)
        self.assertTrue(expected_asks == actual_asks)
        self.assertEqual(expected_stats, actual_stats)

    def test_update_updates_asks_bids_and_stats(self):
        order_book, default_bids, default_asks, default_stats = self.init_test_order_book()

        update_1 = {
            'type': 'l2update',
            'product_id': 'BTC-USD',
            'changes': [],
            'time': '2023-01-01T00:00:01.00000Z'
        }
        order_book.update(update_1)

        expected_bids = default_bids
        expected_asks = default_asks
        expected_stats = default_stats

        actual_bids, actual_asks = order_book.take_snapshot(False)
        actual_stats = order_book.get_stats()

        self.assertTrue(expected_bids == actual_bids)
        self.assertTrue(expected_asks == actual_asks)
        self.assertEqual(expected_stats, actual_stats)

        update_2 = {
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
            'time': '2023-01-01T00:00:02.00000Z'
        }
        order_book.update(update_2)

        expected_bids = ImmutableObjectsPriorityQueue.from_args(
            Bid(15.0, 5.0),
            Bid(14.0, 14.0),
            Bid(10.0, 0.0),
            Bid(3.5, 0.0),
            Bid(2.1, 20.0),
        )
        expected_stats = dataclasses.replace(
            expected_stats,
            highest_bid=Bid(15.0, 5.0),
            operations_diff=dataclasses.replace(expected_stats.operations_diff, bid=Bid(15.0, 5.0),
                                                observed_at='2023-01-01T00:00:02.00000Z')
        )

        actual_bids, actual_asks = order_book.take_snapshot(False)
        actual_stats = order_book.get_stats()
        # TODO: PQs aren't deduplicate records. We need RBTree :facepalm:

        self.assertTrue(expected_bids == actual_bids)
        self.assertTrue(expected_asks == actual_asks)
        self.assertEqual(expected_stats, actual_stats)

    def test_zero_quantity_operations_are_deleted_when_threshold_is_reached(self):
        pass
