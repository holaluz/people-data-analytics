from msilib import schema
import requests
import os 
import sys
import json
import pandas as pd
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..','..','utils')))
#sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
from refresh_token_factorial import get_token

SCHEMA = "people"
TABLE_NAME = "PPL_EMPLOYEES_FT"

token = get_token()
url = "https://api.factorialhr.com/api/v2/core/employees"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

response = requests.request("GET", url, headers=headers)
response_text = response.json()

df = pd.DataFrame(response.json())
df.drop(columns=['termination_reason', 'state','full_name', 'termination_observations','nationality','bank_number', 'postal_code','swift_bic','timeoff_manager_id',
'social_security_number', 'timeoff_policy_id','phone_number','address_line_1', 'address_line_2','updated_at','identifier_type'], inplace=True)
df.drop(columns=['city'], inplace=True)
df.rename(columns = {'created_at':'start_date', 'terminated_on':'end_date','identifier':'factorial_id' }, inplace = True)
df["ceco"] = None
df['team'] = None
df.drop(columns=['team', 'subteam'])


url = "https://api.factorialhr.com/api/v1/core/teams"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df1 = pd.DataFrame(response.json())
df1 = df1.explode('employee_ids')

df2 = pd.merge(df, df1, how='left', left_on='id', right_on='employee_ids')

df1['employee_ids'] = df1['employee_ids'].astype('int64')
df2['id']

df2.drop(columns=['id_y','employee_ids', 'lead_ids'], inplace=True)
df2. rename(columns = {'id_x':'id','name': 'team_name'}, inplace = True)

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgresql_client.write_table(
    df2, 
    "PPL_EMPLOYEES_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)

