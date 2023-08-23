# coinbase-exchange-analyzer

PoC application that runs analyzis on Coinbase cryptocurrency exchange data stream. The application is created as a part
of Kahoot! interview process.

The application connects to
the [level2_batch](https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#level2-batch-channel) data feed for
the given product id (currencies pair sold on the platform, for example "BTC-USD" or "ADA-EUR") and
maintains an order book - a list of all sell requests (asks) and buy requests (bids) for a given exchange auction.

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