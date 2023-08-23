import argparse
import logging
import time

from coinbase.app_logging import print_cmd
from coinbase.order_book_app import OrderBookApp

__DEFAULT_PRODUCT_ID = "BTC-USD"
__DEFAULT_WEB_SOCKET_URL = "wss://ws-feed.pro.coinbase.com"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--product',
        help=f"Product ID which order book is to monitor, default {__DEFAULT_PRODUCT_ID}",
        type=str,
        required=False,
        default=__DEFAULT_PRODUCT_ID
    )
    parser.add_argument(
        '--debug',
        help='Print debug messages into the console',
        required=False,
        action='store_true',
        default=False
    )
    parser.add_argument(
        '--cache',
        help="Store received messages into a specified folder as JSONs",
        type=str,
        required=False
    )
    parser.add_argument(
        '--url',
        help=f"Web Socket URL to connect to, default is {__DEFAULT_WEB_SOCKET_URL}",
        type=str,
        required=False,
        default=__DEFAULT_WEB_SOCKET_URL
    )
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)
    with OrderBookApp(args.url, args.product, args.cache) as app:
        try:
            while True:
                time.sleep(5)
                app.print_stats()
        except KeyboardInterrupt:
            print_cmd("Received the exit signal. Cleaning up the resources and exiting...")
