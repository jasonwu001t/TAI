import pytest
from TAI.broker.ib import IB

def test_ib():
    ib = IB()
    quote = ib.get_stock_quote('AAPL')
    assert quote is not None
    latest_price = ib.get_latest_price('AAPL')
    assert latest_price is not None
