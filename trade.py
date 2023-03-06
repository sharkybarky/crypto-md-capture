import json
import queue
import datetime as dt
from logging import getLogger
from _decimal import Decimal

log = getLogger(__name__)


class Trade:
    def __init__(self,
                 symbol: str,
                 exchange_dt: dt.datetime,
                 received_dt: dt.datetime,
                 price: Decimal,
                 volume: Decimal,
                 counter_volume: Decimal):
        self.symbol = symbol
        self.exchange_timestamp = exchange_dt
        self.received_timestamp = received_dt
        self.price = price
        self.volume = volume
        self.counter_volume = counter_volume

    def to_dict(self):
        my_dict = {}
        for key, value in self.__dict__.items():
            if isinstance(value, (int, float, str, bool, list, tuple, dict, type(None))):
                my_dict[key] = value
            else:
                # Handle unsupported types here. For example, datetime and decimals.
                if isinstance(value, Decimal):
                    my_dict[key] = str(value)
                if isinstance(value, dt.datetime):
                    my_dict[key] = value.isoformat()

        # Serialize the dictionary to JSON format
        return my_dict

    def to_json(self):
        return json.dumps(self.to_dict())


class TradeProcessor:
    def __init__(self, trade_queue: queue.Queue):
        self.trade_queue = trade_queue
        self.last_trade: Trade = None

    def on_trade(self, symbol, order_record: dict, trade_base, trade_counter, exchange_dt, received_dt):
        price = Decimal(order_record['price'])
        trade = Trade(symbol, exchange_dt, received_dt, price, trade_base, trade_counter)
        if self.trade_queue:
            try:
                log.info(f"About to put trade. Trade queue size: {self.trade_queue.qsize()}")
                self.trade_queue.put(trade, block=True, timeout=10)
                log.info(f"Queued trade. Trade queue size: {self.trade_queue.qsize()}")
                self.last_trade = trade
            except queue.Full as e:
                log.exception(f"Queue ran out of space:", e)



