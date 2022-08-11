import redshift_connector
import pandas as pd
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

SCHEMA = "people"
TABLE_NAME = "TAL_CANDIDATES_FT"

conn = redshift_connector.connect(
     host='production-customer-redshift-1y8x.csyrzb3icveb.us-east-1.redshift.amazonaws.com',
     database='w_5fe3463da',
     user='user',
     password='password')

with conn:
    with conn.cursor() as cursor:
        cursor.execute("select id, candidate_created_at, job_title, job_department, job_first_published_at, application_method, current_stage_id, first_screened_at, first_contacted_at, first_interviewed_at, first_offer_at, first_hired_at, disqualified, disqualified_at, recruiter_id, firstname, lastname, email from candidates")
        result = (cursor.fetchall())

print(result)
df = pd.DataFrame(result, columns= ['id','candidate_created_at','job_title','job_department', 
'job_first_published_at','application_method' ,'current_stage_id', 'first_screened_at'
, 'first_contacted_at', 'first_interviewed_at','first_offer_at', 'first_hired_at','disqualified',
'disqualified_at', 'recruiter_id','firstname','lastname', 'email'])
print(df.head())

mysql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
mysql_client.write_table(
    df, 
    "TAL_CANDIDATES_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)