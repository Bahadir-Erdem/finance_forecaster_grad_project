# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(__init__) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import logging
import azure.functions as func
from .Database import Database
from .PredictionsData import PredictionsData

etl_lightgbm_predict_insert_blueprint = func.Blueprint()


@etl_lightgbm_predict_insert_blueprint.timer_trigger(schedule="0 15 14 * * 3", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def etl_lightgbm_predict_insert(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    
    server = 'tcp:gradprojectito.database.windows.net'
    database = 'grad-project'
    username = 'bentham'
    password = '!Rand357' 
    driver = 'Driver={ODBC Driver 18 for SQL Server}'
    # server = 'localhost'
    # database = 'grad_project'
    # username = 'user1'
    # password = '12345' 
    # driver = 'Driver={ODBC Driver 18 for SQL Server}'
    database = Database(driver, server, database, username, password)

    with database.connect() as db:
        predictins_data = PredictionsData(db)
        predictins_data.predict_and_to_db()
        logging.info('Code executed with success!')


    logging.info('Python timer trigger function executed.')