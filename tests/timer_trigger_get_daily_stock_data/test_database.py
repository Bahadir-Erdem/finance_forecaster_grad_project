import pytest
from unittest.mock import patch, MagicMock
from src.timer_trigger_get_daily_stock_data.database import Database
import yaml


@pytest.fixture
def database_obj():
    with open("database_config.yaml") as f:
        yaml_dictionary = yaml.safe_load(f)
        database_settings = yaml_dictionary["test_db_settings"]

    server = database_settings["server"]
    database = database_settings["database"]
    username = database_settings["username"]
    password = database_settings["password"]
    driver = database_settings["driver"]
    conn_str = (
        f"{driver};Server={server},1433;Database={database};Uid={username};Pwd={password};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=300;"
    )

    database = Database(**database_settings)
    database.conn_str = conn_str
    return database


def test_database_connect(database_obj):
    with patch("pyodbc.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        assert database_obj.connect() is not None
        assert database_obj.conn is not None
        assert database_obj.engine is not None


def test_database_close(database_obj):
    with patch("pyodbc.connect") as mock_connect:
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        with database_obj.connect() as db:
            pass

        mock_connection.close.assert_called()


def test_database_context_manager(database_obj):
    with database_obj.connect() as db:
        assert db.conn is not None
        assert db.engine is not None
    assert database_obj.conn.closed is True


def test_database_context_manager_exception(database_obj):
    with patch("pyodbc.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        mock_connect.return_value.cursor.side_effect = Exception("Test Exception")
        with pytest.raises(Exception):
            with database_obj as db:
                db.conn.cursor().execute("SELECT * FROM table")
