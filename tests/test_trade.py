import unittest
from _decimal import Decimal

from trade import Trade
import datetime as dt


class TradeTest(unittest.TestCase):
    def setUp(self):
        self.symbol = "XBTZAR"
        self.tnow = dt.datetime(2022, 1, 12, 4, 5, 6, 123456, None)
        self.msg_time = dt.datetime(2022, 1, 12, 4, 5, 5, 654321, None)

    def test_trade_dict_serialisation(self):
        test_trade = Trade(symbol=self.symbol, exchange_dt=self.msg_time, received_dt=self.tnow,
                           price=Decimal("1.0001"), volume=Decimal("1.5"),
                           counter_volume=Decimal("76000.0"))
        dict_representation = test_trade.to_dict()

        self.assertEqual(dict_representation, {"symbol": "XBTZAR",
                                               "exchange_timestamp": "2022-01-12T04:05:05.654321",
                                               "received_timestamp": "2022-01-12T04:05:06.123456",
                                               "price": "1.0001",
                                               "volume": "1.5",
                                               "counter_volume": "76000.0"})

    def test_trade_json_serialisation(self):
        test_trade = Trade(symbol=self.symbol, exchange_dt=self.msg_time, received_dt=self.tnow,
                           price=Decimal("1.0001"), volume=Decimal("1.5"),
                           counter_volume=Decimal("76000.0"))
        json_representation = test_trade.to_json()
        self.assertEqual(json_representation,
                         '{"symbol": "XBTZAR", '
                         '"exchange_timestamp": "2022-01-12T04:05:05.654321", '
                         '"received_timestamp": "2022-01-12T04:05:06.123456", '
                         '"price": "1.0001", '
                         '"volume": "1.5", '
                         '"counter_volume": "76000.0"}')

