import redshift_connector
import pandas as pd
import os
import yaml
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

SCHEMA = "people"
TABLE_NAME = "TAL_CANDIDATES_FT"

creds_fp = None if os.environ['USERNAME']=='Administrator' else os.path.join(os.environ['USERPROFILE'],'creds','creds_people.yml')
credentials = load_credentials(credentials_fp = creds_fp)
conn = redshift_connector.connect(**credentials['redshift'])

with conn:
    with conn.cursor() as cursor:
        cursor.execute("select id, candidate_created_at, job_title, job_first_published_at, application_method, current_stage_id, first_screened_at, first_contacted_at, first_interviewed_at, first_offer_at, first_hired_at, disqualified, disqualified_at, recruiter_id, firstname, lastname, email from candidates")
        result = (cursor.fetchall())

df = pd.DataFrame(result, columns= ['id','candidate_created_at','job_title', 
'job_first_published_at','application_method' ,'current_stage_id', 'first_screened_at'
, 'first_contacted_at', 'first_interviewed_at','first_offer_at', 'first_hired_at','disqualified',
'disqualified_at', 'recruiter_id','firstname','lastname', 'email'])


with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df.to_csv('C:/Users/Administrator/Desktop/output.csv')
import psycopg2
credentials_postgre = credentials['people_write']
m_dbCon = psycopg2.connect(user=credentials_postgre['username'], password=credentials_postgre['password'], host=credentials_postgre['host'] 
,database=credentials_postgre['database'])
curr = m_dbCon.cursor()
curr.execute('truncate table "people"."TAL_CANDIDATES_FT"')
curr.close()
m_dbCon.commit()
postgresql_client.write_table(
    df, 
    "TAL_CANDIDATES_FT", 
    "people", 
    if_exists = 'append') # see the different values that if_exists can take in the method docsting
