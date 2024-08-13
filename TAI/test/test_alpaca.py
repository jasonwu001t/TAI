import pytest
from TAI.source.alpaca import Alpaca

def test_alpaca():
    alpaca = Alpaca()
    account = alpaca.get_account()
    assert account is not None
    positions = alpaca.get_positions()
    assert positions is not None
