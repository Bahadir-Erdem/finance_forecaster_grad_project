from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime, timedelta

from src.timer_trigger_get_daily_stock_data.date import Date
from src.timer_trigger_get_daily_stock_data.database import Database

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

def test_get_current_quarter(database_obj):
    with database_obj.connect() as db:
        date_obj = Date(db)
        # Test for January
        assert date_obj.get_current_quarter(datetime(2023, 1, 15)) == 1
        # Test for April
        assert date_obj.get_current_quarter(datetime(2023, 4, 1)) == 2
        # Test for July
        assert date_obj.get_current_quarter(datetime(2023, 7, 15)) == 3
        # Test for October
        assert date_obj.get_current_quarter(datetime(2023, 10, 1)) == 4

def test_get_current_date(database_obj):
    with database_obj.connect() as db:
        date_obj = Date(database_obj)
        now = datetime.now()
        date_data = date_obj.get_current_date(now)
        assert date_data['date'] == now.date()
        assert date_data['day'] == now.day
        assert date_data['week'] == now.weekday()
        assert date_data['month'] == now.month
        assert date_data['quarter'] == date_obj.get_current_quarter(now)
        assert date_data['year'] == now.year

@pytest.mark.parametrize("datetime_value, expected_date_data", [
    (datetime(2023, 10, 26), {
        'date': datetime(2023, 10, 26).date(),
        'day': 26,
        'week': 3,
        'month': 10,
        'quarter': 4,
        'year': 2023
    }),
    (datetime(2024, 1, 1), {
        'date': datetime(2024, 1, 1).date(),
        'day': 1,
        'week': 0,
        'month': 1,
        'quarter': 1,
        'year': 2024
    })
])
def test_insert_date(database_obj, datetime_value, expected_date_data):
    with patch('pyodbc.connect') as mock_connect:
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connect.return_value.cursor.return_value = mock_cursor

        with database_obj.connect() as db:
            date_obj = Date(db)
            date_obj.insert_date(datetime_value)
            INSERT_QUERY = f"INSERT INTO dim_date_t ({', '.join(expected_date_data.keys())}) VALUES ({', '.join(['?' for _ in expected_date_data])})"
            mock_cursor.execute.assert_called_once_with(
                INSERT_QUERY,
                tuple(expected_date_data.values())
            )
            mock_connection.commit.assert_called_once()
            mock_cursor.close.assert_called_once()

@pytest.mark.parametrize("datetime_value, existing_date_id, expected_date_id", [
    (datetime(2023, 10, 26), 1, 1),
    (datetime(2024, 1, 1), None, 2)
])
def test_set_date_id(database_obj, datetime_value, existing_date_id, expected_date_id):
    with patch('pyodbc.connect') as mock_connect:
        mock_cursor = mock_connect.return_value.cursor.return_value
        row = {'datetime': datetime_value}
        mock_cursor.fetchone.side_effect = [(existing_date_id,), (expected_date_id,)]

        GET_EXISTING_DATE_QUERY = """
            SELECT date_id
            FROM dim_date_t
            WHERE date = ?
        """

        GET_NEWEST_ID_QUERY = """
            SELECT MAX(date_id) AS max_id
            FROM dim_date_t
        """

        with database_obj.connect() as db:
            date_obj = Date(db)
            date_obj.set_date_id(row)
            mock_cursor.execute.assert_any_call(
                GET_EXISTING_DATE_QUERY, (str(row['datetime'].date()),)
            )
            if existing_date_id is None:
                mock_cursor.execute.assert_any_call(GET_NEWEST_ID_QUERY)
            assert row['date_id'] == expected_date_id