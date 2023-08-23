import dataclasses
import datetime

import numpy as np
import pytest
from sortedcontainers import SortedDict

from coinbase.data_model import OrderBookStats, BidAskDiff, Order
from coinbase.order_book import OrderBook


# TODO: think about proper package structure for tests
@pytest.fixture
def default_test_order_book(sample_rate_sec=1):
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
        Order(14.0, 14.0),
        Order(20.1, 20.0),
        BidAskDiff(14.0, 20.1, datetime.datetime(2023, 1, 1, 0, 0, 0)),
        np.nan,
        {60: mid_price, 5 * 60: mid_price, 15 * 60: mid_price},
        {60: np.nan, 5 * 60: np.nan, 15 * 60: np.nan},
    )

    order_book = OrderBook(snapshot, sample_rate_sec=sample_rate_sec)
    return order_book, expected_bids, expected_asks, expected_stats


def test_order_book_constructor_creates_ordered_asks_and_bids_data_structures(default_test_order_book):
    order_book, expected_bids, expected_asks, expected_stats = default_test_order_book
    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats


def test_update_updates_asks_bids_and_stats(default_test_order_book):
    order_book, default_bids, default_asks, default_stats = default_test_order_book
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

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats

    update = {
        'type': 'l2update',
        'product_id': 'BTC-USD',
        'changes': [
            [
                'buy',
                '10000.0',
                '0.0'
            ],
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
        60: pytest.approx((update_1_mid_price + update_0_mid_price * 60) / (60 + 1)),
        5 * 60: pytest.approx((update_1_mid_price + update_0_mid_price * 60 + default_mid_price) / (1 + 60 + 1)),
        15 * 60: pytest.approx((update_1_mid_price + update_0_mid_price * 60 + default_mid_price) / (1 + 60 + 1))
    }
    expected_stats = dataclasses.replace(
        expected_stats,
        current_highest_bid=Order(15.0, 5.0),
        mid_prices=update_1_mid_prices,
        forecasted_mid_price=pytest.approx(17.058059499870545)
    )

    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats

    update = {
        'type': 'l2update',
        'product_id': 'BTC-USD',
        'changes': [
            [
                'sell',
                '0.01',
                '0.0'
            ],
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
        60: pytest.approx((update_2_mid_price + update_1_mid_price * 60) / (60 + 1)),
        5 * 60: pytest.approx(
            (update_2_mid_price + update_1_mid_price * 4 * 60 + update_0_mid_price * 60) / (5 * 60 + 1)
        ),
        15 * 60: pytest.approx(
            (update_2_mid_price + update_1_mid_price * 4 * 60 + update_0_mid_price * 60 + default_mid_price) / (
                    5 * 60 + 2
            ))
    }
    expected_stats = OrderBookStats(
        Order(15.0, 5.0),
        Order(40.6, 0.6),
        BidAskDiff(15.0, 40.6, datetime.datetime(2023, 1, 1, 0, 5, 1)),
        pytest.approx(17.4829420254619),
        update_2_mid_prices,
        {60: np.nan, 5 * 60: np.nan, 15 * 60: np.nan},
    )

    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats

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
        60: pytest.approx((update_3_mid_price + update_2_mid_price * 60) / (60 + 1)),
        5 * 60: pytest.approx(
            (update_3_mid_price + update_2_mid_price * 5 * 60) / (5 * 60 + 1)
        ),
        15 * 60: pytest.approx(
            (update_3_mid_price
             + update_2_mid_price * 10 * 60
             + update_1_mid_price * 4 * 60
             + update_0_mid_price * 60) / (15 * 60 + 1))
    }
    expected_stats = dataclasses.replace(
        expected_stats,
        current_lowest_ask=Order(40.6, 0.7),
        mid_prices=update_3_mid_prices,
        forecasted_mid_price=pytest.approx(24.34572671931479)
    )

    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats


def test_empty_order_book_return_nans():
    snapshot = {
        'type': 'snapshot',
        'product_id': 'BTC-USD',
        'asks': [],
        'bids': [],
        'time': '2023-01-01T00:00:00.00000Z'
    }

    expected_bids = SortedDict(lambda x: -x)
    expected_asks = SortedDict()
    expected_stats = OrderBookStats(
        Order(np.nan, np.nan),
        Order(np.nan, np.nan),
        BidAskDiff(np.nan, np.nan, datetime.datetime(2023, 1, 1, 0, 0, 0)),
        np.nan,
        {60: np.nan, 5 * 60: np.nan, 15 * 60: np.nan},
        {60: np.nan, 5 * 60: np.nan, 15 * 60: np.nan},
    )

    order_book = OrderBook(snapshot, sample_rate_sec=1)
    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats

    new_orders = {
        'type': 'l2update',
        'product_id': 'BTC-USD',
        'changes': [
            [
                'sell',
                '10.0',
                '1.0'
            ],
            [
                'buy',
                '2.0',
                '0.2'
            ],
            [
                'sell',
                '13.3',
                '3.0'
            ],
            [
                'buy',
                '4.0',
                '4.0'
            ],
        ],
        'time': '2023-01-01T00:0:01.00000Z'
    }
    order_book.update(new_orders)
    matched_orders = {
        'type': 'l2update',
        'product_id': 'BTC-USD',
        'changes': [
            [
                'sell',
                '10.0',
                '0.0'
            ],
            [
                'buy',
                '2.0',
                '0.0'
            ],
            [
                'sell',
                '13.3',
                '0.0'
            ],
            [
                'buy',
                '4.0',
                '0.0'
            ],
        ],
        'time': '2023-01-01T00:00:02.00000Z'
    }
    order_book.update(matched_orders)

    updated_mid_price = (4.0 + 10.0) / 2

    expected_stats = dataclasses.replace(
        expected_stats,
        max_ask_bid_diff=BidAskDiff(4.0, 10.0, datetime.datetime(2023, 1, 1, 0, 0, 1)),
        mid_prices={60: updated_mid_price, 5 * 60: updated_mid_price, 15 * 60: updated_mid_price},
    )

    actual_bids, actual_asks = order_book.take_snapshot()
    actual_stats = order_book.get_stats()

    assert expected_bids == actual_bids
    assert expected_asks == actual_asks
    assert expected_stats == actual_stats



@pytest.mark.skip(
    reason="not implemented, because the functionality isn't essential for the POC project. "
           "The tests will be important important for the production code."
)
def test_take_snapshot_object_cant_be_used_to_update_order_book(self):
    pass
