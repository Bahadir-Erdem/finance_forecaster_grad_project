# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(__init__) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import logging
import azure.functions as func
from .database import Database
from .stock_data_handler import StockDataHandler
import yaml

timer_trigger_get_daily_stock_data_blueprint = func.Blueprint()


@timer_trigger_get_daily_stock_data_blueprint.timer_trigger(schedule="0 15 12 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_get_daily_stock_data(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    with open('database_config.yaml') as f:
        yaml_dictionary = yaml.safe_load(f)
        database_settings = yaml_dictionary['production_db_settings']

    database = Database(**database_settings)

    with database.connect() as db:
        stock_data_handler = StockDataHandler(db)
        stock_data_handler.fetch_and_to_db()
        logging.info('Code executed successfully!')

    logging.info('Python timer trigger function executed.')