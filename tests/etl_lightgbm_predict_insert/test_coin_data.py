import pytest
from src.etl_lightgbm_predict_insert.coin_data import CoinData

BITCOIN_UUID = 'Qwsogvtv82FCd'
ETHERIUM_UUID = 'razxDUgYGNAdQ'

@pytest.fixture
def coin_data():
    return CoinData()

def test_get_coin_uuids(coin_data):
    uuids = coin_data.get_coin_uuids()
    assert isinstance(uuids, list)
    assert len(uuids) > 0

def test_clean_coin_data(coin_data):
    sample_data = {
        'data': {
            'history': [
                {'timestamp': 1621836000, 'price': '5000'},
                {'timestamp': 1621922400, 'price': '5500'}
            ]
        }
    }
    
    cleaned_data = coin_data.clean_coin_data(sample_data, BITCOIN_UUID)
    assert len(cleaned_data) == 2
    assert 'price' in cleaned_data.columns
    assert 'entity' in cleaned_data.columns
    assert 'year' in cleaned_data.columns
    assert 'month' in cleaned_data.columns
    assert 'day' in cleaned_data.columns

def test_get_coin_price_history(coin_data):
    uuids = [BITCOIN_UUID, ETHERIUM_UUID]
    prices = coin_data.get_coin_price_history(uuids)
    assert not prices.empty
    assert len(prices) > 0
    assert uuids in prices['entity'].unique()

def test_get_coin_data(coin_data):
    coin_df = coin_data.get_coin_data()
    assert not coin_df.empty
    assert 'year' in coin_df.columns
    assert 'month' in coin_df.columns
    assert 'day' in coin_df.columns
    assert 'price' in coin_df.columns
