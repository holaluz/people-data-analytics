import redshift_connector

conn = redshift_connector.connect(
     host='production-customer-redshift-1y8x.csyrzb3icveb.us-east-1.redshift.amazonaws.com',
     database='w_5fe3463da',
     user='user',
     password='password')

with conn:
    with conn.cursor() as cursor:
        cursor.execute("select id,job_created_at,title, department,code,state,city,experience,salary_from,salary_to,first_published_at,last_published_at from jobs")
        result = (cursor.fetchall())


import pandas as pd
df = pd.DataFrame(result, columns = ['id','job_created_at','title', 'department','code',
'state','city','experience','salary_from','salary_to',
'first_published_at','last_published_at'])
print(df.head())


from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

mysql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
mysql_client.write_table(
    df, 
    "TAL_JOBS_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)