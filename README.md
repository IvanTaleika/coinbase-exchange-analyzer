# coinbase-exchange-analyzer

PoC application that runs analyzis on Coinbase cryptocurrency exchange data stream. The application is created as a part
of Kahoot! interview process.

The application connects to
the [level2_batch](https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#level2-batch-channel) data feed for
the given product id (currencies pair sold on the platform, for example "BTC-USD" or "ADA-EUR") and
maintains an order book - a list of all sell requests (asks) and buy requests (bids) for a given exchange auction.
Every 5 seconds the user sees the following book statistics:

* Highest bid price and quantity
* Lowest ask price and quantity
* What is the biggest difference between the highest bid price and lowest ask price so far
* Average mid-prices (average price of the current highest bid price and the current lowest ask price) for the last 1, 5
  and 15 minutes
* Forecasted mid-price in 60 seconds
* The forecasting (absolute difference between the predicted price and the observed price once it arrives) for the last
  1, 5 and 15 minutes

Note that forecasting stats aren't available from the beginning. This is because the model needs data to train on.
The model is trained after the first 2 minutes of data is received. Then, it is updated every 6 seconds and completely
retrained every 2 minutes. Additionally, if source data is fits the model poorly, the app will increase (re)training
interval by 1 minute until the model is trained successfully. The app stores at most 15 minutes of data, so data
features with longer seasonality won't be reflected.

## Running the app

The application can be started in the provided docker container. Run

```bash
docker build . -t coinbase-exchange-analyzer:latest
docker run coinbase-exchange-analyzer:latest [args]
```

The application connects to the `BTC-USD` on `wss://ws-feed.pro.coinbase.com` by default. These parameters can be
changed by dedicated command line arguments. Run `docker run coinbase-exchange-analyzer:latest --help` for more details.

## Development

The application was developed and tested for Python 3.11.4. All required libraries are specified in `requirements.txt`
