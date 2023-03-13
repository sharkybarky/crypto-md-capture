import unittest
from decimal import *

from order_book_state import OrderBookState
import datetime as dt

MSG_ASKS_INITIAL_BASIC = [
    {'id': 'ask_order1', 'price': '1.00000000', 'volume': '0.01'},
    {'id': 'ask_order2', 'price': '1.25000000', 'volume': '2.02'}
]

MSG_BIDS_INITIAL_BASIC = [
    {'id': 'bid_order1', 'price': '0.80000000', 'volume': '0.01'},
    {'id': 'bid_order2', 'price': '0.70000000', 'volume': '2.02'}
]

MSG_ASKS_INITIAL_SAME_LEVEL = [
    {'id': 'ask_order1', 'price': '2.00000000', 'volume': '1.01'},
    {'id': 'ask_order2', 'price': '2.00000000', 'volume': '2.02'}
]

MSG_BIDS_INITIAL_SAME_LEVEL = [
    {'id': 'bid_order1', 'price': '1.00000000', 'volume': '1.01'},
    {'id': 'bid_order2', 'price': '1.00000000', 'volume': '2.02'}
]

MSG_CREATE_BID = {'order_id': 'BXBQ7Z82V2PK5ZH', 'type': 'BID', 'price': '0.90000000', 'volume': '1.50'}
MSG_CREATE_ASK = {'order_id': 'BXNR56AAFSFJM5G', 'type': 'ASK', 'price': '0.95000000', 'volume': '1.50'}
MSG_CREATE_ASK2 = {'order_id': 'BXNR56AAFSFJM5G', 'type': 'ASK', 'price': '0.1', 'volume': '0.01'}
MSG_CREATE_ASK3 = {'order_id': 'BXNR56AAFSFJM5G', 'type': 'ASK', 'price': '1.0000000', 'volume': '0.01'}
MSG_CREATE_BID2 = {'order_id': 'BXNR56AAFSFJM5G', 'type': 'BID', 'price': '0.80000000', 'volume': '0.01'}

MSG_DELETE = {'order_id': 'ask_order1'}
MSG_DELETE2 = {'order_id': 'BXNR56AAFSFJM5G'}
MSG_DELETE_INVALID = {'order_id': 'no_record_of_this_order_id'}

MSG_TRADE_SOME = [{'base': '0.00112000', 'counter': '499.8201600000000000', 'maker_order_id': 'ask_order1', 'taker_order_id': 'BXDSJ2B8VQHU7VG', 'order_id': 'ask_order1'}]
MSG_TRADE_ALL_TAKER = [{'base': '0.01', 'counter': '104.440000', 'maker_order_id': 'BXDSJ2B8VQHU7VG', 'taker_order_id': 'bid_order1', 'order_id': 'bid_order1'}]
MSG_TRADE_ALL_MAKER = [{'base': '0.01', 'counter': '104.440000', 'maker_order_id': 'bid_order1', 'taker_order_id': 'BXDSJ2B8VQHU7VG', 'order_id': 'bid_order1'}]


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
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)

        # validate we get the orders from the register okay
        order1 = book_state.book_by_order_register.get('ask_order1')
        order2 = book_state.book_by_order_register.get('ask_order2')
        self.assertIsNotNone(order1)
        self.assertIsNotNone(order2)

        # validate the book_state looks okay
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.8': Decimal('0.01'), '0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1': Decimal('0.01'), '1.25': Decimal('2.02')}, ask_side)

    def test_initial_price_levels_summed(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_SAME_LEVEL, MSG_ASKS_INITIAL_SAME_LEVEL)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'1': Decimal('3.03')}, bid_side)
        self.assertEqual({'2': Decimal('3.03')}, ask_side)

    def test_order_book_on_create_msg(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)
        book_state.on_create(create_msg=MSG_CREATE_ASK, msg_time=self.msg_time, rec_time=self.tnow)
        book_state.on_create(create_msg=MSG_CREATE_BID, msg_time=self.msg_time, rec_time=self.tnow)
        book_state.on_create(create_msg=MSG_CREATE_ASK2, msg_time=self.msg_time, rec_time=self.tnow)
        book_state.on_create(create_msg=MSG_CREATE_ASK3, msg_time=self.msg_time, rec_time=self.tnow)
        book_state.on_create(create_msg=MSG_CREATE_BID2, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        self.assertEqual(Decimal('0.01') + Decimal('0.01'), bid_side['0.8'])
        self.assertEqual(Decimal('2.02'), bid_side['0.7'])
        self.assertEqual(Decimal('1.50'), bid_side['0.9'])

        ask_side = book_state.book.get('ASK')
        self.assertEqual(Decimal('1.5'), ask_side['0.95'])
        self.assertEqual(Decimal('0.01'), ask_side['0.1'])
        self.assertEqual(Decimal('0.01') + Decimal('0.01'), ask_side['1'])
        self.assertEqual(Decimal('2.02'), ask_side['1.25'])

    def test_order_book_on_delete_msg(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)
        book_state.on_delete(delete_msg=MSG_DELETE, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.8': Decimal('0.01'), '0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1.25': Decimal('2.02')}, ask_side)
        book_state.on_create(create_msg=MSG_CREATE_BID2, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.8': Decimal('0.02'), '0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1.25': Decimal('2.02')}, ask_side)
        book_state.on_delete(delete_msg=MSG_DELETE2, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.8': Decimal('0.01'), '0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1.25': Decimal('2.02')}, ask_side)

    def test_order_book_on_delete_thats_not_there(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)

        with self.assertLogs(logger='order_book_state', level='WARNING') as log:
            book_state.on_delete(delete_msg=MSG_DELETE_INVALID, msg_time=self.msg_time, rec_time=self.tnow)
            self.assertIn('WARNING:order_book_state:Received a delete for an order which we have no record!',
                          log.output)

    def test_order_book_on_trade_occurred(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)
        book_state.on_trade(trade_msg=MSG_TRADE_SOME, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.8': Decimal('0.01'), '0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1': Decimal('0.01') - Decimal('0.00112'), '1.25': Decimal('2.02')}, ask_side)

    def test_order_book_on_trade_occurred_resulting_in_zero_volume_at_level(self):
        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)
        book_state.on_trade(trade_msg=MSG_TRADE_ALL_TAKER, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1': Decimal('0.01'), '1.25': Decimal('2.02')}, ask_side)

        book_state = self.construct_book(MSG_BIDS_INITIAL_BASIC, MSG_ASKS_INITIAL_BASIC)
        book_state.on_trade(trade_msg=MSG_TRADE_ALL_MAKER, msg_time=self.msg_time, rec_time=self.tnow)
        bid_side = book_state.book.get('BID')
        ask_side = book_state.book.get('ASK')
        self.assertEqual({'0.7': Decimal('2.02')}, bid_side)
        self.assertEqual({'1': Decimal('0.01'), '1.25': Decimal('2.02')}, ask_side)

    # def test_initial_msg_when_corrupt(self):
    #     pass


if __name__ == '__main__':
    unittest.main()
