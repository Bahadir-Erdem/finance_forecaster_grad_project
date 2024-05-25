from datetime import datetime
import pytest
import pandas as pd
from src.etl_lightgbm_predict_insert.stock_data import StockData



@pytest.fixture
def stock_data():
    return StockData()

def test_scrape_top_stocks(stock_data):
    top_stocks = stock_data.scrape_top_stocks()
    assert isinstance(top_stocks, pd.DataFrame)
    assert len(top_stocks) > 0
    assert 'Symbol' in top_stocks.columns
    assert 'Market Cap' in top_stocks.columns
    assert '% Change' in top_stocks.columns
    assert 'Revenue' in top_stocks.columns

def test_clean_top_stocks(stock_data):
    sample_data = pd.DataFrame({
        'No.': ['1', '2', '3'],
        'Symbol': ['AAPL', 'GOOGL', 'MSFT'],
        'Stock Price': ['190', '160.2', '215.6'],
        'Market Cap': ['1.5B', '1.2B', '1.1B'],
        '% Change': ['5%', '-2%', '0%'],
        'Revenue': ['10B', '7.5B', '3.5B']
    })
    cleaned_data = stock_data.clean_top_stocks(sample_data)
    assert not cleaned_data.empty

def test_get_stock_price_history(stock_data):
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    price_history = stock_data.get_stock_price_history(symbols)
    assert isinstance(price_history, pd.DataFrame)
    assert not price_history.empty
    assert 'symbol' in price_history.columns
    assert price_history['symbol'].isin(symbols).any()
    assert not price_history.isna().values.any()

def test_clean_stock_data(stock_data):
    sample_data = pd.DataFrame({
        'Date': [datetime(year=2020, month=1, day=1), datetime(year=2020, month=2, day=3), datetime(year=2020, month=5, day=7)],
        'Close': [100.0, 105.0, 110.0],
        'symbol': ['AAPL', 'AAPL', 'AAPL']
    })
    cleaned_data = stock_data.clean_stock_data(sample_data)
    assert len(cleaned_data) == 3
    assert 'year' in cleaned_data.columns
    assert 'month' in cleaned_data.columns
    assert 'day' in cleaned_data.columns
    assert 'price' in cleaned_data.columns
    assert 'entity' in cleaned_data.columns

def test_get_stock_data(stock_data):
    stock_dataset = stock_data.get_stock_data()
    assert isinstance(stock_dataset, pd.DataFrame)
    assert not stock_dataset.empty
    assert 'entity' in stock_dataset.columns
    assert 'year' in stock_dataset.columns
    assert 'month' in stock_dataset.columns
    assert 'day' in stock_dataset.columns
    assert 'price' in stock_dataset.columns