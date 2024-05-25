import pytest
from functions.etl_lightgbm_predict_insert.model import Model
import pandas as pd
from numpy import ndarray

@pytest.fixture
def model():
    return Model()

@pytest.fixture
def dataset():
    # Create a sample dataset for testing
    data = {
        'year': [2022, 2022, 2022, 2022, 2022],
        'month': [1, 1, 1, 1, 1],
        'day': [1, 2, 3, 4, 5],
        'price': [10, 20, 30, 40, 50]
    }
    return pd.DataFrame(data)

def test_get_future_dates(model, dataset):
    future_dates = model.get_future_dates(dataset)
    assert isinstance(future_dates, pd.DataFrame)
    assert len(future_dates) == 14
    assert 'year' in future_dates.columns
    assert 'month' in future_dates.columns
    assert 'day' in future_dates.columns

import numpy as np

def test_lightgbm(model, dataset):
    predictions, future_dates = model.lightgbm(dataset)
    assert isinstance(predictions, np.ndarray)
    assert isinstance(future_dates, pd.DataFrame)
    assert len(predictions) == 14
    assert len(future_dates) == 14
    assert 'year' in future_dates.columns
    assert 'month' in future_dates.columns
    assert 'day' in future_dates.columns