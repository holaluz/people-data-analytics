from msilib import schema
import requests
import gspread
import yaml
import os 
import sys
import pandas as pd
import numpy as np
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_google_drive_service_account_credentials
from holaluz_datatools.credentials import load_credentials
sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
print(sys.path)
from refresh_token_factorial import get_token


token = get_token()
url = "https://api.factorialhr.com/api/v2/core/employees"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df_sociedad = pd.DataFrame(response.json())
columns = list(df_sociedad.columns)

print(df_sociedad)

columns = list(df_sociedad.columns)
columns_to_delete_ls = []
columns_to_delete = ['termination_reason', 'state','termination_observations','bank_number', 'postal_code','swift_bic','timeoff_manager_id',
'social_security_number', 'timeoff_policy_id','phone_number','address_line_1', 'address_line_2','updated_at','identifier_type','teams_id']
for col in columns:
    if col in columns_to_delete:
        columns_to_delete_ls.append(col)
df_sociedad.drop(columns=columns_to_delete_ls, inplace=True)
df_sociedad.drop(columns=['city'], inplace=True)
df_sociedad.rename(columns = {'created_at':'start_date', 'terminated_on':'end_date','identifier':'factorial_id','legal_entity_id':'sociedad_id' }, inplace = True)


token = get_token()
url = "https://api.factorialhr.com/api/v1/core/teams"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df1 = pd.DataFrame(response.json())
df_teams = df1.explode('employee_ids')

df_total = pd.merge(df_sociedad, df_teams, how='left', left_on='id', right_on='employee_ids')

df_teams['employee_ids'] = df_teams['employee_ids'].astype('int64')
df_total. rename(columns = {'id_x':'id','name': 'team_name'}, inplace = True)
df_total.drop(columns=['country','company_id',
'team_ids', 'id_y','employee_ids','lead_ids'], inplace=True)

#Replace sociedad id by sociedad name

dict_sociedad = {202:'holaluz_clidom', 132269:'clidom_solar',132271:'katae',135666:'ghc'}
df_total=df_total.replace({'sociedad_id': dict_sociedad})
dict_sociedad_master = {'holaluz_clidom':'Holaluz Clidom', 'clidom_solar':'Clidom Solar','katae':'Katae','ghc':'GHC'}
df_total=df_total.replace({'sociedad_id': dict_sociedad_master})
df_total.rename(columns = {'sociedad_id':'sociedad_name'}, inplace = True)

#Fill split column with split values


#Join split from split table

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
df_split = []
query_split = """select a.split, a.team, b.id  from people.people."OPS_SPLIT_FT" a
join people.people."PPL_EMPLOYEES_FT" b 
on a.team = b.team_name """

for chunk in postgresql_client.make_query(query_split, chunksize=160000):
    df_split.append(chunk)
df_employees = pd.concat(df_split, ignore_index=True)
postgresql_client.close_connection()

df_merge= pd.merge(df_total, df_employees, how= 'left', on= 'id')

df_merge.drop(columns=['team'], inplace=True)



with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df_merge.to_csv('C:/Users/Administrator/Desktop/output.csv')
import psycopg2
credentials_postgre = credentials['people_write']
m_dbCon = psycopg2.connect(user=credentials_postgre['username'], password=credentials_postgre['password'], host=credentials_postgre['host'] 
,database=credentials_postgre['database'])
curr = m_dbCon.cursor()
curr.execute('truncate table "people"."PPL_EMPLOYEES_FT"')
curr.close()
m_dbCon.commit()
postgresql_client.write_table(
    df_merge, 
    "PPL_EMPLOYEES_FT", 
    "people", 
    if_exists = 'append' # see the different values that if_exists can take in the method docsting
)





