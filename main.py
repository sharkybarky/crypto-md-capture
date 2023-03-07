"""Aim: connect to the Luno streaming market data API websocket,
and receive the initial and then the update messages that follow, keeping
the state of N levels of order book
"""
import json
import logging
import datetime as dt
import threading
from concurrent import futures

import websocket
from websocket import WebSocketApp, ABNF

import secret_consts
from gcp.cloud_publisher import GcpRePublisher
from order_book_state import OrderBookState
from utils.utils import setup_logging
import queue


class WSCallbackHandlers:

    def __init__(self, order_book_state: OrderBookState):
        self.order_book_state = order_book_state

    def on_message(self, wsocket: WebSocketApp, message):
        try:
            # extract timestamp
            now_datetime = dt.datetime.utcnow()  # do this early as possible
            msg_data = json.loads(message)
            ts = msg_data.get('timestamp')
            msg_datetime = dt.datetime.utcfromtimestamp(ts / 1000)
            latency = now_datetime - msg_datetime
            log.debug(f"Message latency: {latency} ({msg_datetime=}, {now_datetime=})")

            # process sequence number
            sequence_no = int(msg_data.get('sequence'))
            if sequence_no > self.order_book_state.sequence_num:
                # good sequence, save it
                self.order_book_state.sequence_num = sequence_no
            else:
                # bad sequence, re-init

                # TODO: According to Luno API doc, if an update is received out-of-sequence (for example update sequence
                #  n+2 or n-1 received after update sequence n), the client cannot continue and must reinitialise the
                #  subscription and state. This branch is untested as yet
                log.error(f"Bad sequence number detected. "
                          f"Had seq: {self.order_book_state.sequence_num}, but received: {sequence_no}")
                self.order_book_state.sequence_num = 0
                self.order_book_state.out_of_sequence_restart = True
                wsocket.close()

            # log.debug(f"On message json: {msg_data}")  # print the received message
            asks_initial = msg_data.get("asks")
            bids_initial = msg_data.get("bids")
            trade_update = msg_data.get("trade_updates")
            create_update = msg_data.get("create_update")
            delete_update = msg_data.get("delete_update")
            status_update = msg_data.get("status_update")
            if asks_initial:
                log.info(f"Received initial asks. Depth of book: {len(asks_initial)} asks in total")
                self.order_book_state.on_initial(asks_initial, 'ASK', msg_datetime, now_datetime)
            if bids_initial:
                log.info(f"Received initial bids. Depth of book: {len(bids_initial)} bids in total")
                self.order_book_state.on_initial(bids_initial, 'BID', msg_datetime, now_datetime)
            if trade_update:
                log.debug(f"Received trade update")
                log.debug(f"{trade_update}: {msg_datetime=}")
                self.order_book_state.on_trade(trade_update, msg_datetime, now_datetime)
                # self.trade_processor.on_trade(trade_update, msg_datetime, now_datetime)
            if create_update:
                log.debug(f"Received create update")
                log.debug(f"{create_update}: {msg_datetime=}")
                self.order_book_state.on_create(create_update, msg_datetime, now_datetime)
            if delete_update:
                log.debug(f"Received delete update")
                log.debug(f"{delete_update}: {msg_datetime=}")
                self.order_book_state.on_delete(delete_update, msg_datetime, now_datetime)
            if status_update:
                log.warning(f"Received status update")
                log.warning(f"{status_update}: {msg_datetime=}")

            if not asks_initial and not bids_initial and not trade_update and not create_update and not delete_update \
                    and not status_update:
                log.debug(f"Received null message - json: {msg_data}")  # print the received message

            self.order_book_state.validate_structures()

        except Exception as e:
            log.exception(e)

    @staticmethod
    def on_error(wsocket: WebSocketApp, error_exception):
        log.error(f"Exception: {error_exception}")  # print the error message
        if isinstance(error_exception, Exception):
            raise error_exception
        else:
            raise Exception("received an unknown error")

    @staticmethod
    def on_close(wsocket: WebSocketApp, close_status_code, close_msg):
        log.info(f"Connection closed - status: {close_status_code=} msg: {close_msg=}")

    @staticmethod
    def on_open(wsocket: WebSocketApp):
        log.info("Connection established, sending credentials")
        creds_dict = {"api_key_id": secret_consts.luno_key_id, "api_key_secret": secret_consts.luno_secret}
        json_data = json.dumps(creds_dict, ensure_ascii=False)
        # send a message to the WebSocket server
        wsocket.send(json_data, opcode=ABNF.OPCODE_TEXT)


def start_ws(url, order_book, callbacks, shutdown_evt):
    # creates a WebSocket connection and registers the event handlers
    websocket.enableTrace(False)  # enable debugging output

    ws = websocket.WebSocketApp(url,
                                on_message=callbacks.on_message,
                                on_error=callbacks.on_error,
                                on_close=callbacks.on_close,
                                on_open=callbacks.on_open)
    # start the WebSocket connection and run it in a separate thread
    shutdown_with_error = False
    try:
        shutdown_with_error = ws.run_forever()
    except Exception as e:
        # the only way run_forever unblocks is when an exception is thrown from one of the on_* handlers
        pass

    if shutdown_with_error:
        log.error(f"Websocket shutdown ungracefully")
        log.info("About to set event")
        shutdown_evt.set()
    else:
        log.info("Websocket shutdown")
        if order_book.out_of_sequence_restart:
            log.info("Out of sequence restart needed")
            order_book.out_of_sequence_restart = False
            start_ws(stream_url, order_book, callbacks, shutdown_evt)
        else:
            log.info("About to set event")
            shutdown_evt.set()
            log.info("Good Bye!")


if __name__ == '__main__':
    setup_logging()
    log = logging.getLogger(__name__)
    stream_url = "wss://ws.luno.com/api/1/stream/XBTZAR"
    # stream_url = "wss://ws.luno.com/api/1/stream/ETHZAR"
    book_queue = queue.Queue(maxsize=100)
    trade_queue = queue.Queue(maxsize=10)
    ccy_par = stream_url[stream_url.rfind('/') + 1:]
    _order_book_state = OrderBookState(symbol=ccy_par, render_flag=False, trade_queue=trade_queue)
    _callback_handlers = WSCallbackHandlers(_order_book_state)

    # start a consumer thread which will take the trades off queue and persist
    trade_consumer = GcpRePublisher()
    shutdown_event = threading.Event()  # use as a means of communicating with consumer thread
    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(trade_consumer.consume_and_republish, trade_queue, shutdown_event)
        start_ws(stream_url, _order_book_state, _callback_handlers, shutdown_event)

    # TODO: start another thread which will take n levels of book updates from queue and persist
