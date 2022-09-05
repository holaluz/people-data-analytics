# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 13:09:36 2022

@author: Administrator
"""

from msilib import schema
import requests
import yaml
import os 
import sys
import pandas as pd
from holaluz_datatools.sql import PostgreSQLClient
#from holaluz_datatools.credentials import load_credentials
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..','..','utils')))
#sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
from refresh_token_factorial import get_token

SCHEMA = "people"
TABLE_NAME = "PPL_EMPLOYEES_FT"

token = get_token()
url = "https://api.factorialhr.com/api/v1/employees"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df = pd.DataFrame(response.json())
columns = list(df.columns)
columns_to_delete_ls = []
columns_to_delete = ['termination_reason', 'state','termination_observations','nationality','bank_number', 'postal_code','swift_bic','timeoff_manager_id',
'social_security_number', 'timeoff_policy_id','phone_number','address_line_1', 'address_line_2','updated_at','identifier_type','teams_id']
for col in columns:
    if col in columns_to_delete:
        columns_to_delete_ls.append(col)
df.drop(columns=columns_to_delete_ls, inplace=True)
df.drop(columns=['city'], inplace=True)
df.rename(columns = {'created_at':'start_date', 'terminated_on':'end_date','identifier':'factorial_id' }, inplace = True)
df["ceco"] = None
df['team'] = None
df['status'] = None
df['chapter_name'] = None

df.columns

#Cross ids from employees & teams endpoint from api factorial to bring team_names to employee table

token = get_token()
url = "https://api.factorialhr.com/api/v1/core/teams"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df1 = pd.DataFrame(response.json())
df1 = df1.explode('employee_ids')

df2 = pd.merge(df, df1, how='left', left_on='id', right_on='employee_ids')

df1['employee_ids'] = df1['employee_ids'].astype('int64')
df2.drop(columns=['id_y','employee_ids', 'lead_ids','avatar','company_holiday_ids','location_id','manager_id','hiring',], inplace=True)
df2. rename(columns = {'id_x':'id','name': 'team_name'}, inplace = True)

#Cross ids from employees & contract version endpoint from api factorial to bring job_titles to employee table

"""postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
df_master = []
query_master = """ """select a."Id Req > DNI/NIE", a."Status", a."Job title"
from people.people."PPL_EMPLOYEES_FT" b
left join people."temp"."OPS_MASTER_FT" a on a."Id Req > DNI/NIE" = b.factorial_id"""

""" for chunk in postgresql_client.make_query(query_master, chunksize=160000):
    df_master.append(chunk)
df_masterfile = pd.concat(df_master, ignore_index=True)
postgresql_client.close_connection() 

df3 = pd.merge(df2, df_masterfile, how='left', left_on='id', right_on='id req > dni/nie') """

#credentials = load_credentials(credentials_fp = 'C:/Users/Administrator/creds/creds_people.yml')
with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df2.to_csv('C:/Users/Administrator/Desktop/output.csv')
postgresql_client.make_query('truncate table PPL_EMPLOYEES_FT')
postgresql_client.write_table(
    df2, 
    "PPL_EMPLOYEES_FT", 
    "people", 
    if_exists = 'append' # see the different values that if_exists can take in the method docsting
)


