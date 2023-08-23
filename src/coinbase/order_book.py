from datetime import datetime

import numpy as np
import pandas as pd
from sortedcontainers import SortedDict
from statsmodels.tsa.arima.model import ARIMA

from coinbase.model import OrderBookStats, BidAskDiff, Operation

SOURCE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class OrderBook:
    DEFAULT_AGGREGATION_WINDOWS_MINUTES = [60, 60 * 5, 60 * 15]

    def __init__(self, snapshot: dict, aggregation_windows_sec=None, forecast_window_sec=60, sample_rate_sec=0.05):
        if aggregation_windows_sec is None:
            aggregation_windows_sec = self.DEFAULT_AGGREGATION_WINDOWS_MINUTES
            aggregation_windows_sec.sort()
        self._aggregation_windows = [pd.Timedelta(seconds=m) for m in aggregation_windows_sec]
        self._forecast_window = pd.Timedelta(seconds=forecast_window_sec)
        self._sample_rate = pd.Timedelta(seconds=sample_rate_sec)
        self._bids: SortedDict[float, float] = SortedDict(lambda x: -x)
        self._asks: SortedDict[float, float] = SortedDict()
        self._mid_prices = pd.Series(dtype=float, index=pd.to_datetime([]))
        self._end_window_predictions = pd.Series(dtype=float, index=pd.to_datetime([]))

        self._last_update = self.__parse_request_time(snapshot['time'])

        for price_level_s, quantity_s in snapshot['bids']:
            self.__insert_record('buy', float(price_level_s), float(quantity_s))

        for price_level_s, quantity_s in snapshot['asks']:
            self.__insert_record('sell', float(price_level_s), float(quantity_s))

        self._max_ask_bid_diff = self.__calc_ask_bid_diff()
        self.__update_mid_prices(self._max_ask_bid_diff)
        self.__calculate_forecast()

    def update(self, update: dict):
        self._last_update = self.__parse_request_time(update['time'])

        for side, price_level_s, quantity_s in update['changes']:
            self.__insert_record(side, float(price_level_s), float(quantity_s))

        new_operations_diff = self.__calc_ask_bid_diff()
        self._max_ask_bid_diff = (
            new_operations_diff if new_operations_diff.diff > self._max_ask_bid_diff.diff else self._max_ask_bid_diff
        )
        self.__update_mid_prices(new_operations_diff)
        self.__calculate_forecast()

    def take_snapshot(self) -> (SortedDict[float], SortedDict[float]):
        bids = self._bids.copy()
        asks = self._asks.copy()
        return bids, asks

    def get_stats(self):

        last_update_time_aligned = self._mid_prices.index[-1]
        mid_price_stats = {}
        forecast_errors = {}
        for window in self._aggregation_windows:
            mean_start_time = last_update_time_aligned - window

            window_mid_prices = self._mid_prices[mean_start_time:].rename("mid_price")
            avg_mid_price = window_mid_prices.mean()
            mid_price_stats[window.seconds] = avg_mid_price

            forecast_window_start_time = mean_start_time - self._forecast_window
            forecast_window_end_time = mean_start_time
            window_end_window_predictions = (
                self._end_window_predictions[forecast_window_start_time:forecast_window_end_time]
                .rename("forecast")
            )

            prediction_accuracy = pd.concat([window_mid_prices, window_end_window_predictions], axis=1)
            prediction_error = (prediction_accuracy["forecast"] - prediction_accuracy["mid_price"]).abs().mean()
            forecast_errors[window.seconds] = prediction_error

        forecasted_mid_price = self._end_window_predictions[-1] if not self._end_window_predictions.empty else np.nan

        return OrderBookStats(
            self.__get_first_record('buy'),
            self.__get_first_record('sell'),
            self._max_ask_bid_diff,
            forecasted_mid_price,
            mid_price_stats,
            forecast_errors
        )

    def __update_mid_prices(self, current_diff: BidAskDiff):
        mid_price = (current_diff.highest_bid_price_level + current_diff.lowest_ask_price_level) / 2
        if self._mid_prices.empty:
            # Back filling to alight the first record with the aggregation window
            self._mid_prices = (
                pd.Series([mid_price], index=[current_diff.observed_at]).resample(self._sample_rate).bfill()
            )
        else:
            sampled_increment = pd.concat(
                [self._mid_prices.iloc[-1:], pd.Series([mid_price], index=[current_diff.observed_at])]
            ).resample(self._sample_rate).ffill()
            new_mid_prices = pd.concat([self._mid_prices, sampled_increment[1:]])
            new_mid_prices = new_mid_prices[self.__data_cleanup_time:]
            self._mid_prices = new_mid_prices

    def __calculate_forecast(self):
        # TODO: play with parameters (order)
        # TODO: define min number of records to start forecasting
        if len(self._mid_prices) > 60:
            forecast_window = pd.Timedelta(seconds=60)
            forecast_window_end_time = self._mid_prices.index[-1] + forecast_window
            model = ARIMA(self._mid_prices, order=(5, 1, 0))
            model_fit = model.fit()
            # TODO: It looks like prediction is always a horizontal line. Is this correct?
            # TODO: a site-packages/statsmodels/base/model.py:607: ConvergenceWarning: Maximum Likelihood optimization failed to converge. Check mle_retvals
            #   warnings.warn("Maximum Likelihood optimization failed to " warning is displayed constantly in the console
            prediction = model_fit.forecast(forecast_window_end_time)
            new_end_window_predictions = pd.concat(
                [self._end_window_predictions, prediction[forecast_window_end_time:]]
            )
            new_end_window_predictions = new_end_window_predictions[self.__data_cleanup_time:]
            self._end_window_predictions = new_end_window_predictions

    @property
    def __data_cleanup_time(self) -> datetime:
        # Keeping at most max aggregation window + 1 record
        return self._last_update - self._aggregation_windows[-1] - self._sample_rate

    @staticmethod
    def __parse_request_time(request_time: str) -> datetime:
        return datetime.strptime(request_time, SOURCE_DATETIME_FORMAT)

    def __calc_ask_bid_diff(self) -> BidAskDiff:
        return BidAskDiff(
            self.__get_first_record('buy').price_level,
            self.__get_first_record('sell').price_level,
            self._last_update
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
