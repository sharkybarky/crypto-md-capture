import datetime as dt
import pandas as pd
from logging import getLogger
from decimal import *

from trade import TradeProcessor

MAX_BOOK_DEPTH = 3500
MAX_BOOK_RENDER_DEPTH = 10

log = getLogger(__name__)


class OrderBookState:
    def __init__(self, symbol, render_flag=False, trade_queue=None):
        self.symbol = symbol
        self.book = {'BID': {}, 'ASK': {}}
        self.trade_processor = TradeProcessor(trade_queue)
        self.order_register = {}
        self.sequence_num = 0
        self.out_of_sequence_restart = False
        self.render_book = render_flag

    def on_initial(self, initial_msg: [], side_of_book: str, msg_time: dt.datetime, rec_time: dt.datetime):
        for count, book_details in enumerate(initial_msg):
            price = float(book_details['price'])
            volume = Decimal(book_details['volume'])
            order_id = book_details['id']
            log.debug(f"{count}: {price}@{volume}: {order_id}")

            # add to order record (needed so we can look up details when we receive deletes)
            self.order_register.update({order_id: {'type': side_of_book,
                                                   'price': price,
                                                   'volume': volume,
                                                   'exchange_timestamp': msg_time,
                                                   'received_timestamp': rec_time}
                                        })
            # build book
            volume += self.book[side_of_book].get(price, Decimal(0))  # add this volume to any existing at this level
            self.book[side_of_book].update({price: volume})  # TODO: add id to FILO list?

            # stop when we've processed MAX_BOOK_DEPTH complete price levels
            if len(self.book[side_of_book].keys()) > MAX_BOOK_DEPTH:
                # TODO: delete the last price level as it could contain incomplete volume/ids if
                #  there are multiple orders at this price
                break

        self.render()

    def on_trade(self, trade_msg, msg_time: dt.datetime, rec_time: dt.datetime):
        for trade in trade_msg:
            trade_base = Decimal(trade['base'])
            trade_counter = Decimal(trade['counter'])

            maker_id = trade.get('maker_order_id')
            maker_order = self.order_register.get(maker_id)

            taker_id = trade.get('taker_order_id')
            taker_order = self.order_register.get(taker_id)

            order_id = trade.get('order_id')
            order = self.order_register.get(order_id)
            self.trade_processor.on_trade(self.symbol, order, trade_base, trade_counter, msg_time, rec_time)

            log.debug(f"\n{maker_order=}, \n{taker_order=}, \n{order=}")

            if maker_order:
                # reduce the maker volume by the size of the trade and update orders
                maker_order['volume'] -= trade_base
                log.debug(f"Updated maker order: \n{maker_order=}")
                if maker_order['volume'] == 0:
                    # remove order from order register if its now zero volume
                    self.order_register.pop(maker_id)
                elif maker_order['volume'] < 0:
                    log.error(f"Order volume went negative! \n{maker_order=}")
                    # handle this case if it ever happens. log error to see
                self.order_register.update(maker_order)
                self.update_book_after_trade_or_deletion(maker_order, trade_base)

            if taker_order:
                # reduce the maker volume by the size of the trade
                taker_order['volume'] -= trade_base
                log.debug(f"Updated taker order: \n{taker_order=}")
                if taker_order['volume'] <= 0:
                    log.error("Order volume went to zero or negative!")
                    # handle this case if it ever happens. log error to see
                self.order_register.update(taker_order)
                self.update_book_after_trade_or_deletion(taker_order, trade_base)

            if not maker_order and not taker_order:
                log.error("Trade occurred but no maker or taker order found in order register")

        self.render()

    def update_book_after_trade_or_deletion(self, order, reduce_by):
        # update book
        level = self.book[order['type']].get(order['price'])
        if level:
            new_level = level - reduce_by
            price = order['price']
            if new_level < 0:
                log.error("Somethings gone wrong, price in book reduced below 0!")
            elif new_level > 0:
                self.book[order['type']].update({price: new_level})
            else:
                # remove level entirely
                removed = self.book[order['type']].pop(price)
                log.debug(f"Removed entire level: {removed=}")
        else:
            log.error("Somethings gone wrong updating book, trying to update or remove an order which doesn't exist!")

    def on_create(self, create_msg, msg_time: dt.datetime, rec_time: dt.datetime):
        order_id = create_msg.get('order_id')
        side_of_book = create_msg.get('type')
        price = float(create_msg.get('price'))
        volume = Decimal(create_msg.get('volume'))
        created = {order_id: {'type': side_of_book,
                              'price': price,
                              'volume': volume,
                              'exchange_timestamp': msg_time,
                              'received_timestamp': rec_time}}
        self.order_register.update(created)
        # update book
        volume += self.book[side_of_book].get(price, Decimal(0))  # add this volume to any existing at this level
        self.book[side_of_book].update({price: volume})
        log.debug(f"Order created: \n{created=}")

        self.render()

    def on_delete(self, delete_msg, msg_time: dt.datetime, rec_time: dt.datetime):
        order_id = delete_msg.get('order_id')
        order = self.order_register.get(order_id)
        if order:
            removed = self.order_register.pop(order_id)
            log.debug(f"Order deleted: \n{removed=}")
            # remove reduce volume from book or remove price entirely
            self.update_book_after_trade_or_deletion(order, reduce_by=order['volume'])
        else:
            log.warning("Received a delete for an order which we have no record!")

        self.render()

    def render(self):
        if self.render_book:
            self._render()

    def _render(self):
        if len(self.book['BID']) > 0 and len(self.book['ASK']) > 0:
            # Convert to pd DataFrame for cleaner rendering
            # Create a list of tuples from the dictionary
            list_of_bid_tuples = list(self.book['BID'].items())
            list_of_ask_tuples = list(self.book['ASK'].items())
            # Create a pandas DataFrame from the list of tuples
            bid_book = pd.DataFrame(list_of_bid_tuples, columns=['bid_price', 'bid_volume'])
            bid_book = bid_book.sort_values('bid_price', ascending=False)
            ask_book = pd.DataFrame(list_of_ask_tuples, columns=['ask_price', 'ask_volume'])
            ask_book = ask_book.sort_values('ask_price', ascending=True)
            bid_book.reset_index(drop=True, inplace=True)
            ask_book.reset_index(drop=True, inplace=True)
            pretty_book = pd.concat([bid_book, ask_book], ignore_index=False, axis=1)
            print("\n" * 100)  # hack to render half-decent in-place book updates in Pycharm terminal window
            print(f"\n{pretty_book[0:MAX_BOOK_RENDER_DEPTH]}")

    def validate_structures(self):
        bid_side = self.book.get('BID')
        ask_side = self.book.get('ASK')
        if bid_side and ask_side:
            # check that no level in both sides of book has zero volume
            if 0 in bid_side.values() or 0 in ask_side.values():
                log.error("0 volume detected in book")
                self._render()

            if 0.0 in bid_side.keys() or 0.0 in ask_side.keys():
                log.error("0 price detected in book")
                self._render()

