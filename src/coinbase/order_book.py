import logging
import threading
from datetime import datetime

import numpy as np
import pandas as pd
from pmdarima import auto_arima
from sortedcontainers import SortedDict

from coinbase.data_model import OrderBookStats, BidAskDiff, Order
from coinbase.utils import THREADS_JOIN_TIMEOUT

SOURCE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# In production application these values should be configurable.
# However, this requires validation that is out of the scope of this task
WEBSOCKET_UPDATE_TIME_SEC = 0.05
MID_PRICE_SEASONALITY_M = 10
FORECAST_WINDOW_SEC = 60
DEFAULT_AGGREGATION_WINDOWS_SEC = [60, 60 * 5, 60 * 15]


class OrderBook:

    def __init__(
            self,
            snapshot: dict,
            # useful for testing
            sample_rate_sec: int = WEBSOCKET_UPDATE_TIME_SEC,
    ):
        self.__logger = logging.getLogger(__name__)
        self._aggregation_windows = [pd.Timedelta(seconds=m) for m in DEFAULT_AGGREGATION_WINDOWS_SEC]
        self._forecast_window = pd.Timedelta(seconds=FORECAST_WINDOW_SEC)

        # _mid_price_sample_interval defines time interval for the mid-price sampling.
        # It allows using simple `mean` for average calculation, because all intervals are the same size.
        # the 50 ms interval is specified in websocket feed documentation as update rate. However, data is usually
        # coming slower, but sometime can come faster than the interval.
        self._mid_price_sample_interval = pd.Timedelta(seconds=sample_rate_sec)

        # The original intention was to use the _mid_price_sample_interval for the forecast sampling as well.
        # However, this makes the model training too slow. The model assumes seasonality of 1 minute.
        # 6 seconds sampling interval aligns nicely with the seasonality.
        self._forecast_sample_interval = pd.Timedelta(seconds=FORECAST_WINDOW_SEC / MID_PRICE_SEASONALITY_M)

        # Creating the model and retraining it after 2 full seasons of data passed
        self._n_observation_to_retrain_after = MID_PRICE_SEASONALITY_M * 2 + 1
        self._n_model_updates = 0
        self._model_retrain_lock = threading.Lock()

        # Required to understand how many values to predict
        self._model_update_time = None
        self._retrain_model_thread = None
        self._forecast_model = None

        # If update arrive at the middle of the `_mid_price_sample_interval` window, we are delaying it to the start
        # of the next window. These means `_mid_prices` can contain a mid-price from the future. If data is then sent
        # faster than the sample rate we remove the future values from `_mid_prices` and replace it with the actual
        # mid-price for the given interval. Additionally, this allows to reprocess the same update multiple times
        self._last_update = self.__parse_request_time(snapshot['time'])

        self._bids: SortedDict[float, float] = SortedDict(lambda x: -x)
        self._asks: SortedDict[float, float] = SortedDict()
        for price_level_s, quantity_s in snapshot['bids']:
            self.__insert_record('buy', float(price_level_s), float(quantity_s))

        for price_level_s, quantity_s in snapshot['asks']:
            self.__insert_record('sell', float(price_level_s), float(quantity_s))

        self._mid_prices = pd.Series(dtype=float, index=pd.to_datetime([]))
        self._forecast = pd.DataFrame({
            "mid_price": pd.Series(dtype=float),
            "forecast_mid_price": pd.Series(dtype=float),
            "forecast_error": pd.Series(dtype=float),
            "used_in_training": pd.Series(dtype=bool)
        }, index=pd.to_datetime([])
        )
        self._max_ask_bid_diff = self.__update_data_structures()

        self.__logger.debug(
            f"Initialized order book using a snapshot from {self._last_update}. "
            f"Order Book size: {len(self._bids)} bids and {len(self._asks)} asks"
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._retrain_model_thread is not None:
            self.__logger.debug(
                f"Model recalculation is in process. "
                f"Waiting for it to finish gracefully. "
                f"Max waiting time is {THREADS_JOIN_TIMEOUT} seconds"
            )
            self._retrain_model_thread.join(THREADS_JOIN_TIMEOUT)

    def update(self, update: dict):
        self._last_update = self.__parse_request_time(update['time'])

        for side, price_level_s, quantity_s in update['changes']:
            self.__insert_record(side, float(price_level_s), float(quantity_s))

        new_order_diff = self.__update_data_structures()
        self._max_ask_bid_diff = (
            new_order_diff
            if new_order_diff.diff > self._max_ask_bid_diff.diff or np.isnan(self._max_ask_bid_diff.diff)
            else self._max_ask_bid_diff
        )
        self.__logger.debug(
            f"Updated Order Book at {self._last_update}. "
            f"Order Book size: {len(self._bids)} bids and {len(self._asks)} asks."
        )

    def take_snapshot(self) -> (SortedDict[float], SortedDict[float]):
        bids = self._bids.copy()
        asks = self._asks.copy()
        return bids, asks

    @property
    def _forecast_mid_price(self) -> float:
        return self._forecast.iloc[-1]["forecast_mid_price"] if not self._forecast.empty else np.nan

    def get_stats(self) -> OrderBookStats:
        def __calculate_window_means(series: pd.Series, end_time: datetime):
            means = {}
            for window in self._aggregation_windows:
                window_start_time = end_time - window
                window_data = series[window_start_time:]
                means[window.seconds] = window_data.mean()
            return means

        last_update_time_aligned = self._mid_prices.index[-1]
        mid_price_means = __calculate_window_means(self._mid_prices, last_update_time_aligned)

        if self._forecast.empty:
            forecast_error_means = {window.seconds: np.nan for window in self._aggregation_windows}
        else:
            forecast_with_known_mid_prices = self._forecast[:last_update_time_aligned]
            last_forecast_known_mid_price_time_aligned = forecast_with_known_mid_prices.index[-1]
            forecast_error_means = __calculate_window_means(
                forecast_with_known_mid_prices["forecast_error"],
                last_forecast_known_mid_price_time_aligned
            )

        return OrderBookStats(
            self.__get_first_record('buy'),
            self.__get_first_record('sell'),
            self._max_ask_bid_diff,
            self._forecast_mid_price,
            mid_price_means,
            forecast_error_means
        )

    def __update_data_structures(self):
        order_diff = self.__calc_ask_bid_diff()
        self.__update_mid_prices(order_diff)
        train_increment = self.__update_forecast_with_train_data()
        self.__update_model()
        self.__calculate_forecast(train_increment.index)
        self.__free_resources()
        return order_diff

    def __update_mid_prices(self, current_diff: BidAskDiff):
        mid_price = (current_diff.highest_bid_price_level + current_diff.lowest_ask_price_level) / 2

        historical_mid_prices = self._mid_prices[:self._last_update]
        # in an unlikely scenario that update aligns perfectly with the sample_rate interval, removing existing
        # mid-price for that time
        historical_mid_prices = (
            historical_mid_prices
            if not historical_mid_prices.empty and historical_mid_prices.index[-1] != current_diff.observed_at
            else historical_mid_prices.iloc[:-1]
        )

        if historical_mid_prices.empty:
            # valid_mid_prices filling to alight the first record with the aggregation window
            new_mid_prices = (
                pd.Series([mid_price], index=[current_diff.observed_at]).resample(
                    self._mid_price_sample_interval).bfill()
            )
        else:
            sampled_increment = pd.concat(
                [historical_mid_prices.iloc[-1:], pd.Series([mid_price], index=[current_diff.observed_at])]
            ).resample(self._mid_price_sample_interval, closed="right").ffill()
            new_mid_prices = pd.concat([historical_mid_prices, sampled_increment[1:]])

        self._mid_prices = new_mid_prices

    def __update_forecast_with_train_data(self) -> pd.Series:
        increment_series = pd.Series(dtype=float, index=pd.to_datetime([]))

        if self._forecast.empty:
            # Initializing forecast data as soon as the first _forecast_sample_interval is closed
            resampled_mid_prices = self._mid_prices.resample(self._forecast_sample_interval).mean()
            if len(resampled_mid_prices) > 1:
                increment_series = resampled_mid_prices.iloc[:-1]
        else:
            # on each subsequent interval, adding the mean to the training data
            # latest_train_data_time = (
            #     self._model_update_time
            #     if self._model_update_time is not None
            #     else self._forecast[:self._last_update].index[-1]
            # )
            next_forecast_interval_start = self._model_update_time + self._forecast_sample_interval
            next_forecast_interval_end = next_forecast_interval_start + self._forecast_sample_interval
            if len(self._mid_prices[next_forecast_interval_end:]) > 0:
                increment_series = (
                                       self._mid_prices[next_forecast_interval_start: next_forecast_interval_end]
                                       .resample(self._forecast_sample_interval).mean()
                                   )[:-1]

        increment_series = increment_series.rename("mid_price")
        new_forecast = self._forecast.join(increment_series, how="outer", sort=True, rsuffix="_r")
        new_forecast.loc[increment_series.index, "mid_price"] = increment_series
        new_forecast.loc[increment_series.index, "used_in_training"] = False
        self._forecast = new_forecast.drop(columns=["mid_price_r"])

        return increment_series

    def __update_model(self):
        if not self._model_retrain_lock.locked() and not self._forecast.empty:
            # used_in_training is NaN for forecasted values
            not_used_in_training_filter = self._forecast["used_in_training"] == False

            if self._n_model_updates >= self._n_observation_to_retrain_after:
                # Running full retraining asynchronously
                self._model_retrain_lock.acquire()
                self._retrain_model_thread = threading.Thread(
                    target=lambda: self.__retrain_model(
                        self._forecast.loc[:self._model_update_time, "mid_price"].copy())
                )
                self._retrain_model_thread.start()

            else:
                # Running incremental update synchronously
                new_training_data = self._forecast[not_used_in_training_filter]

                if not new_training_data.empty:
                    self._model_update_time = new_training_data.index[-1]
                    self._n_model_updates += len(new_training_data)
                    if self._forecast_model is not None:
                        self._forecast_model.update(new_training_data["mid_price"])

            self._forecast.loc[not_used_in_training_filter, "used_in_training"] = True

    def __retrain_model(self, train_data):
        self.__logger.debug(f"Retraining the forecast SARIMA model using {len(train_data)} data points")
        try:
            self._forecast_model = auto_arima(
                train_data,
                # p and q that seems to be fine for the data when experimenting
                start_p=1,
                start_q=1,
                max_p=5,
                max_q=5,
                # seasonality settings
                test='adf',
                m=MID_PRICE_SEASONALITY_M,
                seasonal=True,
                error_action='raise',
            )
            self._n_model_updates = 0
            self.__logger.debug("The forecast SARIMA model is retrained successfully")
        except Exception as e:
            # I have once observed that auto_arima haven't been able to fit the model.
            # The error was thrown on live data and caching wasn't enabled. I wasn't able to reproduce it.
            # As a precaution, increasing the number of points required for model training.
            self._n_observation_to_retrain_after += MID_PRICE_SEASONALITY_M
            self.__logger.debug(
                f"Failed to (re)train the forecast SARIMA model of data {train_data}"
                f"Will retry after additional {MID_PRICE_SEASONALITY_M} data points. "
                f"Error: {e}", exc_info=True
            )
        finally:
            self._model_update_time = train_data.index[-1]
            self._model_retrain_lock.release()

    def __calculate_forecast(self, new_intervals):
        if self._forecast_model is None:
            return

        new_predictions_index = pd.date_range(
            self._model_update_time,
            self._model_update_time + self._forecast_window,
            freq=self._forecast_sample_interval
        )[1:]

        predictions = pd.Series(
            self._forecast_model.predict(n_periods=len(new_predictions_index)),
            index=new_predictions_index,
            name="forecast_mid_price"
        )

        new_forecast = self._forecast.join(predictions, how="outer", sort=True, rsuffix="_r")
        new_forecast.loc[predictions.index, "forecast_mid_price"] = predictions
        self._forecast = new_forecast.drop(columns=["forecast_mid_price_r"])
        self._forecast.loc[new_intervals, "forecast_error"] = abs(
            self._forecast.loc[new_intervals, "forecast_mid_price"] -
            self._forecast.loc[new_intervals, "mid_price"]
        )

    def __free_resources(self):
        if not self._mid_prices.empty:
            self._mid_prices = self._mid_prices[
                               self._last_update - self._aggregation_windows[-1] - self._mid_price_sample_interval:
                               ]
        if not self._forecast.empty:
            self._forecast = self._forecast[
                             self._last_update - self._aggregation_windows[-1] - self._forecast_sample_interval:
                             ]

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

    def __get_first_record(self, side) -> Order:
        book = self.__get_book(side)
        record = book.peekitem(index=0) if book else (np.nan, np.nan)
        return Order(*record)

    def __insert_record(self, side: str, price_level: float, quantity: float):
        book = self.__get_book(side)
        if quantity > 0:
            book[price_level] = quantity
        elif price_level in book:
            del book[price_level]
