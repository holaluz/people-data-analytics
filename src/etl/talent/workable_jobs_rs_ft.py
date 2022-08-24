import redshift_connector
import pandas as pd
import os
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

SCHEMA = "people"
TABLE_NAME = "TAL_JOBS_FT"
creds_fp = None if os.environ['USERNAME']=='Administrator' else os.path.join(os.environ['USERPROFILE'],'creds','creds_people.yml')
credentials = load_credentials(credentials_fp = creds_fp)
conn = redshift_connector.connect(**credentials['redshift'])

with conn:
    with conn.cursor() as cursor:
        cursor.execute("select id,job_created_at,title, department,code,state,city,experience,salary_from,salary_to,first_published_at,last_published_at from jobs")
        result = (cursor.fetchall())

df = pd.DataFrame(result, columns = ['id','job_created_at','title', 'department','code',
'state','city','experience','salary_from','salary_to',
'first_published_at','last_published_at'])

mysql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
mysql_client.write_table(
    df, 
    "TAL_JOBS_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)