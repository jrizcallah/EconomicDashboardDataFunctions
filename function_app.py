import logging
import azure.functions as func
from config import (BUSINESS_ENTITIES_CSV_PATH,
                    BUSINESS_STATISTICS_CSV_PATH)
from get_data import (save_business_entities,
                      save_business_statistics)


app = func.FunctionApp()


@app.blob_output(arg_name='BusinessEntitiesBlob',
                 path='data/business_entities.csv',
                 connection='BlobConnectionString')
@app.blob_output(arg_name='BusinessStatisticsBlob',
                 path='data/business_statistics.csv',
                 connection='BlobConnectionString')
@app.timer_trigger(schedule="0 0 4 * * *", arg_name="myTimer",
                   run_on_startup=False,
                   use_monitor=False) 
def TimerDataPipeline(myTimer: func.TimerRequest,
                      BusinessEntitiesBlob: func.Out[str],
                      BusinessStatisticsBlob: func.Out[str],) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    save_business_entities()
    save_business_statistics()

    logging.info('Reading Business Entities Data.')
    with open(BUSINESS_ENTITIES_CSV_PATH, 'r') as file:
        entities_text = file.read()
    logging.info('Pushing Business Entities to Blob.')
    BusinessEntitiesBlob.set(entities_text)
    logging.info('Success!')

    logging.info('Reading Business Statistics Data.')
    with open(BUSINESS_STATISTICS_CSV_PATH, 'r') as file:
        statistics_text = file.read()
    logging.info('Pushing Business Statistics to Blob.')
    BusinessStatisticsBlob.set(statistics_text)
    logging.info('Success!')

    logging.info('Python timer trigger function executed.')
