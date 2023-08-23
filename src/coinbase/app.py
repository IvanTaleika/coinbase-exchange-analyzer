import time

from coinbase.order_book_app import OrderBookApp

if __name__ == "__main__":
    ws_url = "wss://ws-feed.pro.coinbase.com"
    product_id = "BTC-USD"
    with OrderBookApp(ws_url, product_id) as app:
        while True:
            time.sleep(5)
            app.print_stats()
