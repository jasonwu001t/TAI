import pytest
from TAI.source.atom import Atom

def test_atom():
    atom = Atom()
    historical_data = atom.get_historical_data('AAPL', '1d', '2023-01-01', '2023-01-31')
    assert historical_data is not None
