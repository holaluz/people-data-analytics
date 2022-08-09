import requests
import os 
import sys
import json
import pandas as pd
sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
from refresh_token_factorial import get_token
json.loads(response.decode('utf-8'))[0]

token = get_token()
url = "https://api.factorialhr.com/api/v2/core/employees"

payload={}
headers = {'Authorization': f'Bearer {token}'}
print(headers)

response = requests.request("GET", url, headers=headers, data=payload)
response_text = response.json()

df = pd.DataFrame(json.loads(response.text))
df.drop(columns=['termination_reason', 'state','full_name', 'termination_observations','nationality','bank_number', 'postal_code','swift_bic','timeoff_manager_id',
'social_security_number', 'timeoff_policy_id','phone_number','address_line_1', 'address_line_2','updated_at','identifier_type'], inplace=True)
df.drop(columns=['identifier_type','address_line_1', 'address_line_2'], inplace=True)
df.drop(columns=['nationality','city'], inplace=True)
df.rename(columns = {'created_at':'start_date', 'terminated_on':'end_date','identifier':'factorial_id' }, inplace = True)
df.columns
df["ceco"] = None
df.columns
df['team'] = None
df.drop(columns=['team', 'subteam'])


url = "https://api.factorialhr.com/api/v1/core/teams"

payload={}
headers = {'Authorization': f'Bearer {token}'}
print(headers)

response = requests.request("GET", url, headers=headers, data=payload)
response_text = response.json()

df1 = pd.DataFrame(json.loads(response.text))
df1.head()
df1.columns
df1 = df1.explode('employee_ids')

df2 = pd.merge(df, df1, how='left', left_on='id', right_on='employee_ids')
df.id.dtype
df1.employee_ids.dtype
df1['employee_ids'] = df1['employee_ids'].astype('int64')

df2['id']
df2.columns
df.columns

df2.drop(columns=['id_y','employee_ids', 'lead_ids'], inplace=True)
df2. rename(columns = {'id_x':'id','name': 'team_name'}, inplace = True)
df2.columns

from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgresql_client.write_table(
    df2, 
    "PPL_EMPLOYEES_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)

