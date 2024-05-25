import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.timer_trigger_get_daily_stock_data.stock_data_fetcher import StockDataFetcher  # Replace with the actual module name

@pytest.fixture
def fetcher_instance():
    return StockDataFetcher()

def test_fetch(fetcher_instance):
    with patch.object(fetcher_instance, 'scrape_top_stocks') as mock_scrape, \
         patch.object(fetcher_instance, 'clean_top_stocks') as mock_clean_top, \
         patch.object(fetcher_instance, 'get_stock_data') as mock_get_data, \
         patch.object(fetcher_instance, 'clean_stock_data') as mock_clean_data:
        
        mock_scrape.return_value = pd.DataFrame({'Symbol': ['AAPL', 'MSFT']})
        mock_clean_top.return_value = pd.Series(['AAPL', 'MSFT'])
        mock_get_data.return_value = pd.DataFrame({'symbol': ['AAPL', 'MSFT'], 'companyName': ['Apple', 'Microsoft']})
        mock_clean_data.return_value = pd.DataFrame({'symbol': ['AAPL', 'MSFT'], 'company_name': ['Apple', 'Microsoft']})

        result = fetcher_instance.fetch()

        mock_scrape.assert_called_once()
        mock_clean_top.assert_called_once_with(mock_scrape.return_value)
        mock_get_data.assert_called_once_with(mock_clean_top.return_value)
        mock_clean_data.assert_called_once_with(mock_get_data.return_value)
        assert result.equals(mock_clean_data.return_value)

def test_scrape_top_stocks(fetcher_instance):
    mock_html = """
    <table>
      <tr><th>Symbol</th><th>Name</th><th>Market Cap</th></tr>
      <tr><td>AAPL</td><td>Apple Inc.</td><td>2.1T</td></tr>
      <tr><td>MSFT</td><td>Microsoft Corp.</td><td>2.0T</td></tr>
    </table>
    """
    with patch('requests.get') as mock_get:
        mock_get.return_value = MagicMock(text=mock_html)
        result = fetcher_instance.scrape_top_stocks()
        expected_df = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT'],
            'Name': ['Apple Inc.', 'Microsoft Corp.'],
            'Market Cap': ['2.1T', '2.0T']
        })
        pd.testing.assert_frame_equal(result, expected_df)

def test_clean_top_stocks(fetcher_instance):
    top_stocks_df = pd.DataFrame({
        'No.': [1, 2],
        'Symbol': ['AAPL', 'MSFT'],
        'Market Cap': ['2.1B', '2.0B'],
        'Revenue': ['100B', '90B'],
        '% Change': ['1.5%', '2.0%'],
        'Stock Price': ['60000', '65000']
    })
    result = fetcher_instance.clean_top_stocks(top_stocks_df)
    assert result.equals(pd.Series(['AAPL', 'MSFT']))

def test_get_newest_stock_prices(fetcher_instance):
    symbol = 'AAPL'
    mock_response = {'c': 150.0, 'h': 155.0, 'l': 145.0, 'o': 148.0, 'pc': 149.0, 't': 1622217600}
    with patch('requests.get') as mock_get:
        mock_get.return_value.json = MagicMock(return_value=mock_response)
        result = fetcher_instance.get_newest_stock_prices(symbol)
        assert result == mock_response

def test_get_stock_info(fetcher_instance):
    symbol = 'AAPL'
    mock_response = [{'symbol': 'AAPL', 'companyName': 'Apple Inc.'}]
    with patch('requests.get') as mock_get:
        mock_get.return_value.json = MagicMock(return_value=mock_response)
        result = fetcher_instance.get_stock_info(symbol)
        assert result == mock_response[0]

def test_get_stock_data(fetcher_instance):
    symbols = ['AAPL', 'MSFT']
    with patch.object(fetcher_instance, 'get_stock_info') as mock_get_info, \
         patch.object(fetcher_instance, 'get_newest_stock_prices') as mock_get_prices:

        mock_get_info.side_effect = [
            {'symbol': 'AAPL', 'companyName': 'Apple Inc.'},
            {'symbol': 'MSFT', 'companyName': 'Microsoft Corp.'}
        ]
        mock_get_prices.side_effect = [
            {'c': 150.0, 'h': 155.0, 'l': 145.0, 'o':148.0, 'pc': 149.0, 't': 1622217600},
            {'c': 250.0, 'h': 255.0, 'l': 245.0, 'o': 248.0, 'pc': 249.0, 't': 1622217600}
        ]

        result = fetcher_instance.get_stock_data(symbols)
        expected_df = pd.DataFrame([
            {'symbol': 'AAPL', 'companyName': 'Apple Inc.', 'c': 150.0, 'h': 155.0, 'l': 145.0, 'o': 148.0, 'pc': 149.0, 't': 1622217600},
            {'symbol': 'MSFT', 'companyName': 'Microsoft Corp.', 'c': 250.0, 'h': 255.0, 'l': 245.0, 'o': 248.0, 'pc': 249.0, 't': 1622217600}
        ])
        pd.testing.assert_frame_equal(result, expected_df)

def test_clean_stock_data(fetcher_instance):
    df = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT'],
        'companyName': ['Apple Inc.', 'Microsoft Corp.'],
        'exchangeShortName': ['NASDAQ', 'NASDAQ'],
        'image': ['url1', 'url2'],
        'industry': ['Tech', 'Tech'],
        't': [1622217600, 1622217600],
        'c': [150.0, 250.0],
        'h': [155.0, 255.0],
        'l': [145.0, 245.0],
        'o': [148.0, 248.0]
    })
    
    result = fetcher_instance.clean_stock_data(df)
    expected_df = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT'],
        'company_name': ['Apple Inc.', 'Microsoft Corp.'],
        'exchange': ['NASDAQ', 'NASDAQ'],
        'icon_url': ['url1', 'url2'],
        'industry': ['Tech', 'Tech'],
        'datetime': [datetime.fromtimestamp(1622217600), datetime.fromtimestamp(1622217600)],
        'close': [150.0, 250.0],
        'high': [155.0, 255.0],
        'low': [145.0, 245.0],
        'open': [148.0, 248.0],
        'date_id': [None, None],
        'time_id': [None, None]
    })
    pd.testing.assert_frame_equal(result, expected_df)