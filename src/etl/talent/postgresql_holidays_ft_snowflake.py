"""
Creates the table PPL_HOLIDAYS_FT in Snowflake database.
Sources: postgresql
Output: snowflake

"""
from datetime import date
import logging
import pandas as pd
import yaml
from holaluz_datatools import SQLServerClient, SnowflakeSQLClient, PostgreSQLClient
from holaluz_datatools.utils import load_query, set_logger_config
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.s3 import S3Resource

SCHEMA = 'PUBLIC'
TABLE_NAME = 'PPL_PEOPLE_SOLAR_HOLIDAYS_FT'
S3_BUCKET = 'curated-data-dl'
S3_PATH = 'historic/people/export'
S3_FILENAME = 'ppl_people_solar_holidays'

logger = logging.getLogger(__name__)
#credentials = load_credentials()
with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)

def main():

    ### get data from postgresql ###
    query_holidays = 'select * from people.people.solar_holidays_ft shf'
    logger.info(f'Getting client data from view solar staff holidays')
    with PostgreSQLClient(**credentials['people_write']) as core_client:
        df_hols = core_client.make_query(query_holidays, chunksize = 1000) 
        df_hols = pd.concat(df_hols)

    ### push data to S3 ###
    s3_resource = S3Resource(**credentials['s3'])
    logger.info(f'pushing data into S3 {S3_BUCKET!r} bucket, in the {S3_PATH!r} folder')
    s3_resource.push_to_s3(
        df_or_dict = df_hols, 
        bucket = S3_BUCKET, 
        path_s3 = S3_PATH,
        filename = S3_FILENAME
    )

    ### copy S3 data into Snowflake ###
    logger.info(f'copying data into Snowflake {TABLE_NAME!r} table')
    with SnowflakeSQLClient(**credentials['snowflake_dw_write']) as snowflake_client:
        snowflake_client.copy_into(
            path_s3 = S3_PATH, 
            filename = S3_FILENAME, 
            table_name = TABLE_NAME, 
            schema = SCHEMA,
            df= df_hols #only when new 
        )

if __name__ == '__main__':
    set_logger_config()
    main()

