import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from pytz import timezone
import yaml
from src.timer_trigger_get_daily_stock_data.database import Database
from src.timer_trigger_get_daily_stock_data.time import Time


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


def test_get_time(database_obj):
    with patch("datetime.datetime") as mock_datetime:
        sample_datetime = datetime(2023, 5, 24, 10, 30, 45, tzinfo=timezone("Turkey"))
        mock_datetime.now.return_value = sample_datetime
        with database_obj.connect() as db:
            time_obj = Time(db)
            current_time = time_obj.get_time(sample_datetime)
            assert current_time == {
                "time": datetime.time(mock_datetime.now.return_value),
                "hour": 10,
                "minute": 30,
                "second": 45,
            }


def test_insert_time(database_obj):
    with patch("pyodbc.connect") as mock_connect:

        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_fetchone = mock_cursor.execute.return_value.fetchone
        mock_fetchone.return_value = [123]

        mock_cursor.execute.side_effect = [
            mock_fetchone,
            mock_cursor.execute.return_value,
        ]

        time_collection = {
            "time": datetime.time(datetime(2023, 5, 24, 10, 30, 45)),
            "hour": 10,
            "minute": 30,
            "second": 45,
        }

        with database_obj.connect() as db:
            time_id = Time(db).insert_time(time_collection)

        TIME_TABLE_NAME = "dim_time_t"
        columns = ", ".join(time_collection.keys())
        placeholders = ", ".join(["?" for _ in time_collection])
        INSERT_QUERY = (
            f"INSERT INTO {TIME_TABLE_NAME} ({columns}) VALUES ({placeholders})"
        )
        GET_NEWEST_ID_QUERY = "SELECT @@IDENTITY AS ID;"

        assert mock_cursor.execute.call_args_list[0][0][0] == INSERT_QUERY

        mock_cursor.execute.assert_called_with(GET_NEWEST_ID_QUERY)

        mock_connect.return_value.commit.assert_called_once()
        assert time_id == 123


def test_set_time_id(database_obj):
    with patch("pyodbc.connect") as mock_connect:
        mock_cursor = mock_connect.return_value.cursor.return_value

        mock_cursor.fetchone.return_value = [456]
        row = {
            "datetime": datetime(2023, 5, 24, 10, 30, 45, tzinfo=timezone("Turkey")),
            "date_id": 999,
            "time_id": None,
        }
        with database_obj.connect() as db:
            time_obj = Time(db)
            time_obj.set_time_id(row)
            assert row["time_id"] == 456
            mock_cursor.execute.assert_called_with(
                "SELECT time_id FROM dim_time_t WHERE time = ?",
                (
                    datetime.time(
                        datetime(2023, 5, 24, 10, 30, 45, tzinfo=timezone("Turkey"))
                    ),
                ),
            )

            with patch.object(Time, "insert_time") as mock_insert_time:
                mock_cursor.fetchone.return_value = [None]
                mock_insert_time.return_value = 789
                time_obj.set_time_id(row)
                assert row["time_id"] == 789
                mock_insert_time.assert_called_with(
                    {
                        "time": datetime.time(
                            datetime(2023, 5, 24, 10, 30, 45, tzinfo=timezone("Turkey"))
                        ),
                        "hour": 10,
                        "minute": 30,
                        "second": 45,
                    }
                )
