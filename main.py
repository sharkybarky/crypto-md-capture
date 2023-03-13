"""Aim: connect to the Luno streaming market data API websocket,
and receive the initial and then the update messages that follow, keeping
the state of N levels of order book
"""
import logging
import threading
from concurrent import futures
import queue

import ws_handlers
from gcp.cloud_publisher import GcpRePublisher
from order_book_state import OrderBookState
from utils.utils import setup_logging


if __name__ == '__main__':
    setup_logging()
    log = logging.getLogger(__name__)
    stream_url = "wss://ws.luno.com/api/1/stream/XBTZAR"
    # stream_url = "wss://ws.luno.com/api/1/stream/ETHZAR"
    book_queue = queue.Queue(maxsize=100)
    trade_queue = queue.Queue(maxsize=10)
    ccy_par = stream_url[stream_url.rfind('/') + 1:]
    _order_book_state = OrderBookState(symbol=ccy_par, render_flag=False, trade_queue=trade_queue)
    _callback_handlers = ws_handlers.WebsocketCallbackHandlers(_order_book_state)

    # start a consumer thread which will take the trades off queue and persist
    trade_consumer = GcpRePublisher()
    shutdown_event = threading.Event()  # use as a means of communicating with consumer thread
    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(trade_consumer.consume_and_republish, trade_queue, shutdown_event)
        ws_handlers.start_ws(stream_url, _order_book_state, _callback_handlers, shutdown_event)

    # TODO: start another thread which will take n levels of book updates from queue and persist
