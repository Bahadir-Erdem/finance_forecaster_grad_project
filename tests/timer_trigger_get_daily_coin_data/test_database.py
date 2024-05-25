import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock

from functions.timer_trigger_get_daily_coin_data.database import Database, Date, Time, CoinData, CoinPriceData

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

@pytest.fixture
def database_obj():
    server = 'localhost'
    database = 'grad_project'
    username = 'user1'
    password = '12345'
    driver = 'Driver={ODBC Driver 18 for SQL Server}'
    conn_str = (f'{driver};Server={server},1433;Database={database};Uid={username};Pwd={password};'
                'Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=300;'
                )
    database = Database('localhost',
             'grad_project',
             'user1', '12345',
             'Driver={ODBC Driver 18 for SQL Server}'    
    )
    database.conn_str = conn_str
    database.retry_delay = 1
    return database


def test_database_connect(database_obj):
    assert database_obj.connect() is not None

def test_database_insert_time(database_obj, time_obj):
    database_obj.connect()
    time_id = database_obj.insert_time(time_obj)
    assert time_id is not None
    database_obj.close()

def test_database_insert_date(database_obj, date_obj):
    database_obj.connect()
    date_id = database_obj.insert_date(date_obj)
    assert date_id is not None
    database_obj.close()

def test_database_add_date(database_obj, date_obj):
    database_obj.connect()
    database_obj.add_date(date_obj)
    GET_LATEST_DATE_QUERY = '''SELECT MAX(date) AS LatestDate
    FROM dim_date_t;'''
    
    with database_obj.conn.cursor() as cursor:
        cursor.execute(GET_LATEST_DATE_QUERY)
        result = cursor.fetchone()[0]
        assert datetime.now().date() == result
    database_obj.connect()
    

        
@patch('pyodbc.connect')
def test_database_commit(mock_connect, database_obj, coin_data_obj):
    mock_conn = MagicMock()

    mock_connect.return_value = mock_conn

    database_obj.connect()

    database_obj.insert_or_update_coin_data(coin_data_obj)

    mock_conn.commit.assert_called_once()

    database_obj.close()


@patch('pyodbc.connect')
def test_database_insert_or_update_coin_data_ft(mock_connect, database_obj, coin_price_data_obj):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    database_obj.connect()

    database_obj.insert_or_update_coin_data_ft(coin_price_data_obj)

    mock_connect().commit.assert_called_once()

    database_obj.close()
