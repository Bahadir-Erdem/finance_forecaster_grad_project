# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(__init__) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import logging
import azure.functions as func
from .database import Database
from .predictions_data import PredictionsData
import yaml

etl_lightgbm_predict_insert_blueprint = func.Blueprint()


@etl_lightgbm_predict_insert_blueprint.timer_trigger(schedule="0 0 6 * * 1", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def etl_lightgbm_predict_insert(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    with open('database_config.yaml') as f:
        yaml_dictionary = yaml.safe_load(f)
        database_settings = yaml_dictionary['production_db_settings']

    database = Database(**database_settings)

    with database.connect() as db:
        predictins_data = PredictionsData(db)
        predictins_data.predict_and_to_db()
        logging.info('Code executed with success!')

    logging.info('Python timer trigger function executed.')