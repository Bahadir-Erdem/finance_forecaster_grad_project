import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.timer_trigger_get_daily_stock_data.database import Database
from src.timer_trigger_get_daily_stock_data.time import Time
from src.timer_trigger_get_daily_stock_data.date import Date
from src.timer_trigger_get_daily_stock_data.stock_data_fetcher import StockDataFetcher
from src.timer_trigger_get_daily_stock_data.stock_data_handler import StockDataHandler

FILE_ADDRESS = 'functions.timer_trigger_get_daily_stock_data'

@pytest.fixture
def database_obj():
    server = 'localhost'
    database = 'grad_project'
    username = 'user1'
    password = '12345'
    driver = 'Driver={ODBC Driver 18 for SQL Server}'
    conn_str = (f'{driver};Server={server},1433;Database={database};Uid={username};Pwd={password};'
                'Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=300;')

    database = Database(driver, server, database, username, password)
    database.conn_str = conn_str
    return database

@pytest.fixture
def date_obj(database_obj):
    return Date(database_obj)

@pytest.fixture
def time_obj(database_obj):
    return Time(database_obj)

@pytest.fixture
def stock_data_fetcher_obj():
    return StockDataFetcher()

@pytest.fixture
def stock_data_handler_obj(database_obj):
    return StockDataHandler(database_obj)


def test_fetch_and_to_db(database_obj):
    with database_obj.connect() as db:
        with patch.object(Date, 'set_date_id') as mock_set_date_id, \
             patch.object(Time, 'set_time_id') as mock_set_time_id, \
             patch.object(StockDataHandler, 'insert_or_update_stock_data') as mock_insert_or_update_stock_data, \
             patch.object(StockDataHandler, 'insert_or_update_stock_data_ft') as mock_insert_or_update_stock_data_ft:

            stock_data_handler_obj = StockDataHandler(db)
            stock_data_handler_obj.fetch_and_to_db()

            mock_set_date_id.assert_called()
            mock_set_time_id.assert_called()
            mock_insert_or_update_stock_data.assert_called()
            mock_insert_or_update_stock_data_ft.assert_called()

def test_insert_or_update_stock_data(database_obj):
    with patch('pyodbc.connect') as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        # mock_cursor.fetchone.return_value = None

        row = pd.Series({
            'symbol': 'AAPL',
            'company_name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'icon_url': 'https://financialmodelingprep.com/image-stock/AAPL.png',
            'industry': 'Consumer Electronics',
            'datetime': pd.Timestamp('2023-04-01 09:00:00'),
            'close': 123.45,
            'high': 125.00,
            'low': 122.50,
            'open': 123.00,
            'date_id': 1,
            'time_id': 1
        })
        with database_obj.connect() as db:
            stock_data_handler = StockDataHandler(db)
            stock_data_handler.insert_or_update_stock_data(row)

        mock_cursor.execute.assert_called()
        mock_connect.return_value.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

def test_insert_or_update_stock_data_ft(database_obj):
    with patch('pandas.read_sql') as mock_read_sql, \
         patch('pandas.DataFrame.to_sql') as mock_to_sql:
        mock_read_sql.return_value = pd.DataFrame({
            'column_name': [
                'symbol', 'company_name', 'exchange', 'icon_url', 'industry',
                'datetime', 'close', 'high', 'low', 'open', 'date_id', 'time_id'
            ]
        })

        row = pd.Series({
            'symbol': 'AAPL',
            'company_name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'icon_url': 'https://financialmodelingprep.com/image-stock/AAPL.png',
            'industry': 'Consumer Electronics',
            'datetime': pd.Timestamp('2023-04-01 09:00:00'),
            'close': 123.45,
            'high': 125.00,
            'low': 122.50,
            'open': 123.00,
            'date_id': 1,
            'time_id': 1
        })

        with database_obj.connect() as db:
            stock_data_handler = StockDataHandler(db)
            stock_data_handler.insert_or_update_stock_data_ft(row)

            mock_read_sql.assert_called_with(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'ft_stock_price_t'",
                stock_data_handler.engine
            )
            mock_to_sql.assert_called_with(
                'ft_stock_price_t',
                stock_data_handler.engine,
                index=False,
                if_exists='append'
            )