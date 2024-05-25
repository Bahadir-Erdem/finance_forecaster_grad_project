import pytest
import pandas as pd
from functions.etl_lightgbm_predict_insert.TrainingData import TrainingData

@pytest.fixture
def training_data():
    return TrainingData()

def test_get_training_data(training_data):
    training_data_df = training_data.get_training_data()
    assert isinstance(training_data_df, pd.DataFrame)
    assert not training_data_df.empty
    assert not training_data_df.isnull().values.any()
    assert 'entity' in training_data_df.columns
    assert 'year' in training_data_df.columns
    assert 'month' in training_data_df.columns
    assert 'day' in training_data_df.columns
    assert 'price' in training_data_df.columns

def test_get_stock_data(training_data):
    stock_data = training_data.stock_data.get_stock_data()
    assert isinstance(stock_data, pd.DataFrame)
    assert not stock_data.empty
    assert not stock_data.isnull().values.any()
    assert 'entity' in stock_data.columns
    assert 'year' in stock_data.columns
    assert 'month' in stock_data.columns
    assert 'day' in stock_data.columns
    assert 'price' in stock_data.columns

def test_get_coin_data(training_data):
    coin_data = training_data.coin_data.get_coin_data()
    assert isinstance(coin_data, pd.DataFrame)
    assert not coin_data.empty
    assert 'entity' in coin_data.columns
    assert 'year' in coin_data.columns
    assert 'month' in coin_data.columns
    assert 'day' in coin_data.columns
    assert 'price' in coin_data.columns