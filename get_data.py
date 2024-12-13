from config import (BUSINESS_ENTITIES_URL,
                    BUSINESS_STATISTICS_URL,
                    BUSINESS_ENTITIES_CSV_PATH,
                    BUSINESS_STATISTICS_CSV_PATH,
                    GRAPH_DATA_CSV_PATH)
import logging
import pandas as pd
import requests
import re
import json


logger = logging.getLogger(__name__)
logging.basicConfig(filename='get_data.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def download_business_entity_data():
    logger.info(f'Getting Business Entity Data from {BUSINESS_ENTITIES_URL}.')
    r = requests.get(BUSINESS_ENTITIES_URL)
    logger.info(f'Request returned {r}.')
    r_json = json.loads(r.content)
    df = pd.DataFrame.from_records(r_json)
    logger.info(f'Entites DataFrame has {df.shape[0]:,} rows.')
    return df


def download_business_statistics_data():
    logger.info(f'Getting Business Statistics Data from {BUSINESS_STATISTICS_URL}.')
    r = requests.get(BUSINESS_STATISTICS_URL)
    logger.info(f'Request returned {r}.')
    temp = re.split('(Period,Value)', str(r.content))
    temp = ''.join(temp[1:])
    temp = temp.replace('\\r', '')
    temp = temp.replace('\\n', ' ')
    temp = temp.split()

    df = pd.DataFrame.from_records([i.split(',') for i in temp[1:]])
    df.columns = temp[0].split(',')
    df.dropna(inplace=True)
    df = df[df['Value'] != 'NA']
    logger.info(f'Statistics DataFrame has {df.shape[0]:,} rows.')
    
    return df


def clean_business_entities_data(df):
    logging.info('Cleaning business entities data.')
    df['entityformdate'] = pd.to_datetime(df['entityformdate'])
    df['entityformdate'] = df['entityformdate'].astype(str)
    df['count_entityid'] = df['count_entityid'].astype(int)
    df['principalcity'] = df['principalcity'].str.capitalize()
    df.loc[df['principalcity'] == 'None', 'principalcity'] = 'Unknown'
    logging.info(f'Column [principalcity] has {df["principalcity"].isna().sum():,} missing values.')
    df['principalcity'] = df['principalcity'].fillna('Unknown')

    logging.info(f'Column [principalzipcode] has {df["principalzipcode"].isna().sum():,} missing values.')
    df['principalzipcode'] = df['principalzipcode'].fillna('Unknown')
    return df    


def clean_business_statistics_data(df):
    logger.info('Cleaning business statistics data.')
    df['Period'] = pd.to_datetime(df['Period'], format='%b-%Y')
    df['Period'] = df['Period'].astype(str)
    df['Value'] = df['Value'].astype(int)
    logger.info(f'Column [Period] has {df["Period"].isna().sum():,} missing values.')
    logger.info(f'Column [Value] has {df["Value"].isna().sum():,} missing values.')
    df.dropna(inplace=True)
    return df

def make_main_graph_data(business_entity_data,
                         business_statistics_data):
    def prep_business_entity_data(data):
        if data['entityformdate'].dtype != 'datetime64[ns]':
            data['entityformdate'] = pd.to_datetime(data['entityformdate'])
        data['formationmy'] = data['entityformdate'].dt.year.astype(str) + '-' + data['entityformdate'].dt.month.astype(str)
        data['formationmy'] = pd.to_datetime(data['formationmy'])
        formations_by_month = data[['formationmy', 'count_entityid']].groupby('formationmy').sum()
        return formations_by_month
    def prep_business_statistics_data(data):
        if data['Period'].dtype != 'datetime64[ns]':
            data['Period'] = pd.to_datetime(data['Period'])
        if data['Value'].dtype != 'int':
            data['Value'] = data['Value'].astype(int)
        data = data.set_index('Period')
        return data

    logging.info('Combining Business Entity and Statistics Data...')
    business_entity_data = prep_business_entity_data(business_entity_data)
    business_statistics_data = prep_business_statistics_data(business_statistics_data)
    graph_data = business_statistics_data.merge(business_entity_data,
                                                left_index=True, right_index=True)
    graph_data.columns = ['Business Statistics', 'Business Entities']
    logging.info('Changing data to long format.')
    graph_data = graph_data.reset_index().melt(id_vars=['index'], 
                                               value_vars=['Business Statistics', 'Business Entities'])
    graph_data.columns = ['month', 'series', 'value']
    logging.info(f'Main Graph Data has {graph_data.shape[0]} rows.')
    return graph_data


def save_business_entities():
    business_entities = download_business_entity_data()
    business_entities = clean_business_entities_data(business_entities)
    business_entities.to_csv(BUSINESS_ENTITIES_CSV_PATH, index=False)
    logger.info(f'Saved Business Entity Data to {BUSINESS_ENTITIES_CSV_PATH}')
    return business_entities


def save_business_statistics():
    business_statistics = download_business_statistics_data()
    business_statistics = clean_business_statistics_data(business_statistics)
    business_statistics.to_csv(BUSINESS_STATISTICS_CSV_PATH, index=False)
    logger.info(f'Saved Business Statistics Data to {BUSINESS_STATISTICS_CSV_PATH}')
    return business_statistics


def get_data():
    business_entities = save_business_entities()
    business_statistics = save_business_statistics()
    graph_data = make_main_graph_data(business_entities, business_statistics)
    graph_data.to_csv(GRAPH_DATA_CSV_PATH, index=False)


if __name__ == '__main__':
    get_data()
