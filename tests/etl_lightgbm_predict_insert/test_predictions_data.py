import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.etl_lightgbm_predict_insert.database import Database
from src.etl_lightgbm_predict_insert.model import Model
from src.etl_lightgbm_predict_insert.training_data import TrainingData
from src.etl_lightgbm_predict_insert.predictions_data import PredictionsData


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
def model_obj():
    return Model()

@pytest.fixture
def training_data_obj():
    return TrainingData()

@pytest.fixture
def lightgbm_return_value():
    return ([1000, 2000, 300], pd.DataFrame({
            'year': [2023, 2023, 2023],
            'month': [5, 5, 5],
            'day': [1, 2, 3]
    }).copy())

def test_predict_and_to_db(database_obj):
    with patch.object(TrainingData, 'get_training_data') as mock_get_training_data, \
         patch.object(Model, 'lightgbm') as mock_lightgbm, \
         patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        
        mock_get_training_data.return_value = pd.DataFrame({
            'entity': ['btc', 'eth', 'ltc'],
            'year': [2021, 2021, 2021],
            'month': [4, 4, 4],
            'day': [1, 2, 3],
            'price': [50000, 2000, 300]
        })

        mock_lightgbm.side_effect = [
            ([1000, 2000, 300], pd.DataFrame({
                'year': [2023, 2023, 2023],
                'month': [5, 5, 5],
                'day': [1, 2, 3]
            })),
            ([3000, 4000, 500], pd.DataFrame({
                'year': [2023, 2023, 2023],
                'month': [6, 6, 6],
                'day': [1, 2, 3]
            })),
            ([5000, 6000, 700], pd.DataFrame({
                'year': [2022, 2023, 2024],
                'month': [6, 6, 6],
                'day': [1, 2, 3]
            }))
        ]

        with database_obj.connect() as db:
            predictions_data_obj = PredictionsData(db)
            result = predictions_data_obj.predict_and_to_db()

            assert len(predictions_data_obj.results) == 3
            for result in predictions_data_obj.results:
                assert 'predicted_values' in result.columns
                assert 'entity' in result.columns
                assert 'date' in result.columns

            mock_to_sql.assert_called_once_with('predictions_t', predictions_data_obj.engine, if_exists='replace', index=False)