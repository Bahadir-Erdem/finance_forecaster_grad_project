import pytest
import pandas as pd
import datetime
from unittest.mock import patch, MagicMock
from pytz import timezone

from functions.timer_trigger_get_daily_coin_data.entity import Date, Time, CoinData, CoinPriceData

@pytest.fixture
def date_obj():
    return Date()

@pytest.fixture
def time_obj():
    return Time()

@pytest.fixture
def coin_data_obj():
    return CoinData()

@pytest.fixture
def coin_price_data_obj(coin_data_obj):
    return CoinPriceData(coin_data_obj, 1, 1)

def test_date_get_current_quarter(date_obj):
    date_dict = date_obj.get_current_date()
    date_df = pd.DataFrame(date_dict, index=[0])
    date_df['date'] = pd.to_datetime(date_df['date'], format='%y-%m-%d')
    assert date_obj.get_current_quarter() == pd.Timestamp(date_df.loc[0, 'date']).quarter

def test_date_get_current_date(date_obj):
    current_date = date_obj.get_current_date()
    assert isinstance(current_date['date'], datetime.date)
    assert 1 <= current_date['day'] <= 31
    assert 1 <= current_date['week'] <= 53
    assert 1 <= current_date['month'] <= 12
    assert 1 <= current_date['quarter'] <= 4
    assert current_date['year'] == datetime.datetime.now(timezone('Turkey')).year

def test_time_get_current_time(time_obj):
    current_time = time_obj.get_current_time()
    assert isinstance(current_time['time'], datetime.time)
    assert 0 <= current_time['hour'] <= 23
    assert 0 <= current_time['minute'] <= 59
    assert 0 <= current_time['second'] <= 59

def test_coin_data_clean_coin_data(coin_data_obj):
    coins_df = coin_data_obj.clean_coin_data()
    assert isinstance(coins_df, pd.DataFrame)
    assert len(coins_df) == 5
    assert all(col in coins_df.columns for col in ['uuid', 'symbol', 'name', 'icon_url', 'price', 'change', 'rank'])
    assert coins_df['price'].dtype == 'float64'
    assert coins_df['change'].dtype == 'float64'

def test_coin_price_data_get_coins_price(coin_price_data_obj):
    coins_price_df = coin_price_data_obj.get_coins_price()
    assert isinstance(coins_price_df, pd.DataFrame)
    assert 'date_id' in coins_price_df.columns
    assert 'time_id' in coins_price_df.columns