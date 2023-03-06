import unittest
from _decimal import Decimal

from order_book_state import OrderBookState
import datetime as dt

msg_asks_initial_basic = [
    {'id': 'ask_order1', 'price': '1.00000000', 'volume': '0.01'},
    {'id': 'ask_order2', 'price': '1.25000000', 'volume': '2.02'}
]

msg_bids_initial_basic = [
    {'id': 'bid_order1', 'price': '0.80000000', 'volume': '0.01'},
    {'id': 'bid_order2', 'price': '0.70000000', 'volume': '2.02'}
]

msg_asks_initial_same_level = [
    {'id': 'ask_order1', 'price': '2.00000000', 'volume': '1.01'},
    {'id': 'ask_order2', 'price': '2.00000000', 'volume': '2.02'}
]

msg_bids_initial_same_level = [
    {'id': 'bid_order1', 'price': '1.00000000', 'volume': '1.01'},
    {'id': 'bid_order2', 'price': '1.00000000', 'volume': '2.02'}
]

msg_create_ask = {'order_id': 'BXNR56AAFSFJM5G', 'type': 'ASK', 'price': '442091.00000000', 'volume': '0.01500000'}
msg_create_bid = {'order_id': 'BXBQ7Z82V2PK5ZH', 'type': 'BID', 'price': '441055.00000000', 'volume': '0.01350000'}


class OrderBookStateTest(unittest.TestCase):
    def setUp(self):
        # these are needed in every test
        self.tnow = dt.datetime.now()
        self.msg_time = self.tnow - dt.timedelta(seconds=5)

    def construct_book(self, msg_bid, msg_ask):
        book = OrderBookState(symbol="DummySymbol")
        book.on_initial(initial_msg=msg_bid, side_of_book='BID', msg_time=self.msg_time,
                        rec_time=self.tnow)
        book.on_initial(initial_msg=msg_ask, side_of_book='ASK', msg_time=self.msg_time,
                        rec_time=self.tnow)
        return book

    def test_initial_basic(self):
        book_state = self.construct_book(msg_bids_initial_basic, msg_asks_initial_basic)

        # validate we get the orders from the register okay
        order1 = book_state.order_register.get('ask_order1')
        order2 = book_state.order_register.get('ask_order2')
        self.assertIsNotNone(order1)
        self.assertIsNotNone(order2)

        # validate the book_state looks okay
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual(bid_side, {0.8: Decimal('0.01'), 0.7: Decimal('2.02')})
        self.assertEqual(ask_side, {1.0: Decimal('0.01'), 1.25: Decimal('2.02')})

    def test_initial_price_levels_summed(self):
        book_state = self.construct_book(msg_bids_initial_same_level, msg_asks_initial_same_level)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual(bid_side, {1.0: Decimal('3.03')})
        self.assertEqual(ask_side, {2.0: Decimal('3.03')})

    def test_order_book_on_create_msg(self):
        book_state = self.construct_book(msg_bids_initial_basic, msg_asks_initial_basic)
        # book_state.on_create()


    def test_order_book_on_delete_msg(self):
        pass

    def test_order_book_on_delete_thats_not_there(self):
        pass

    def test_order_book_on_trade_occurred(self):
        pass

    def test_order_book_on_trade_occurred_resulting_in_zero_volume_at_level(self):
        pass

    def test_initial_corrupt_calls(self):
        pass


if __name__ == '__main__':
    unittest.main()
