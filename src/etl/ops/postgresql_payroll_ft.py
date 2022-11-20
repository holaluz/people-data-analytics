from msilib import schema
import requests
import os 
import sys
import json
import yaml
import pandas as pd
import numpy as np
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
#sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), '..','..','utils')))
sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
print(sys.path)
from refresh_token_factorial import get_token

SCHEMA = "people"
TABLE_NAME = "OPS_PAYROLL_FT"

token = get_token()
url = "https://api.factorialhr.com/api/v1/payroll/contract_versions"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df = pd.DataFrame(response.json())

"""
selectedCols = df.columns[selected]
print(df[selectedCols])
print(f"Distinct entries: {len(df.groupby(['es_contract_type_id']))}")
"""

df['working_hours']= df['working_hours']/100
df['salary_amount']= df['salary_amount']/100

df.drop(columns=['has_payroll','salary_frequency','es_has_teleworking_contract', 'es_cotization_group',
'es_contract_observations','es_job_description','es_working_day_type_id',
'es_education_level_id', 'es_professional_category_id',
'fr_employee_type', 'fr_forfait_jours', 'fr_jours_par_an',
'fr_coefficient', 'fr_contract_type_id', 'fr_level_id', 'fr_step_id',
'fr_mutual_id', 'fr_professional_category_id', 'fr_work_type_id',
'de_contract_type_id'], inplace=True)

#Join employees for legal_entity


token = get_token()
url = "https://api.factorialhr.com/api/v2/core/employees"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df_sociedad = pd.DataFrame(response.json())

columns = list(df_sociedad.columns)
columns_to_delete_ls = []
columns_to_delete = ['termination_reason', 'state','termination_observations','bank_number', 'postal_code','swift_bic','timeoff_manager_id',
'social_security_number', 'timeoff_policy_id','phone_number','address_line_1', 'address_line_2','updated_at','identifier_type','teams_id']
for col in columns:
    if col in columns_to_delete:
        columns_to_delete_ls.append(col)
df_sociedad.drop(columns=columns_to_delete_ls, inplace=True)
df_sociedad.rename(columns = {'legal_entity_id':'sociedad_id' }, inplace = True)
df_sociedad.drop(columns=['email', 'birthday_on','terminated_on', 'identifier', 'gender', 'nationality', 'country','city', 'company_id','created_at', 'manager_id','team_ids'], inplace = True)

df_total = pd.merge(df, df_sociedad, how='left', left_on='employee_id', right_on='id')


#Replace sociedad id by sociedad name

dict_sociedad = {202:'holaluz_clidom', 132269:'clidom_solar',132271:'katae',135666:'ghc'}
df_total=df_total.replace({'sociedad_id': dict_sociedad})
dict_sociedad_master = {'holaluz_clidom':'Holaluz Clidom', 'clidom_solar':'Clidom Solar','katae':'Katae','ghc':'GHC'}
df_total=df_total.replace({'sociedad_id': dict_sociedad_master})
df_total.rename(columns = {'sociedad_id':'sociedad_name'}, inplace = True)

df_total. rename(columns = {'id_x':'id','es_contract_type_id': 'tipo_contrato'}, inplace = True)
df_total.drop(columns=['id_y'], inplace=True)


conditions = [(df_total['sociedad_name'] == 'Holaluz Clidom') & (df_total['tipo_contrato'] == 1227),
(df_total['sociedad_name'] == 'Holaluz Clidom') & (df_total['tipo_contrato'] == 1228),
(df_total['sociedad_name'] == 'Clidom Solar') & (df_total['tipo_contrato'] == 337917),
(df_total['sociedad_name'] == 'Clidom Solar') & (df_total['tipo_contrato'] == 337918),
(df_total['sociedad_name'] == 'Katae') & (df_total['tipo_contrato'] == 337925),
(df_total['sociedad_name'] == 'Katae') & (df_total['tipo_contrato'] == 337926),
(df_total['sociedad_name'] == 'GHC') & (df_total['tipo_contrato'] == 342964),
(df_total['sociedad_name'] == 'GHC') & (df_total['tipo_contrato'] == 342964)]

choices  = [ "indefinido", 'temporal', 'indefinido','temporal','indefinido','temporal','indefinido','temporal']    
df_total["tipo_contrato"] = np.select(conditions, choices, default=np.nan)

"""df_total.drop(columns=['has_payroll','es_has_teleworking_contract',
'es_cotization_group', 'es_contract_observations', 'es_job_description',
'es_working_day_type_id', 'es_education_level_id','es_professional_category_id', 'fr_employee_type', 'fr_forfait_jours',
'fr_jours_par_an', 'fr_coefficient', 'fr_contract_type_id','fr_level_id', 'fr_step_id', 'fr_mutual_id',
'fr_professional_category_id', 'fr_work_type_id', 'de_contract_type_id' ], inplace=True)

df_total.drop(columns=['salary_frequency'], inplace=True)

#print(df_total['tipo_contrato'].unique())"""

#Join with bonus

"""token = get_token()
url = "https://api.factorialhr.com/api/v1/payroll/supplements"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)
df_bonus = pd.DataFrame(response.json())

df_merge = pd.merge(df, df_bonus, how='inner', on='employee_id')

df_merge = pd.merge(df, df_bonus, how='left', on='employee_id')

#df_merge = pd.concat([df, df_bonus], keys=["employee_id", 'amount_in_cents'])

df_merge.drop(columns=['id_y','effective_on_y','id_y','contracts_compensation_id', 'contracts_taxonomy_id','unit', 'updated_at', 'validated','description_entity', 'created_by_access_id','payroll_policy_period_id'], inplace=True)
df_merge.rename(columns = {'effective_on_x':'effective_on','amount_in_cents': 'bonus'}, inplace = True)
df_merge.rename(columns = {'id_x':'id'}, inplace = True)
df_merge['bonus']= df_merge['bonus']/100"""


#credentials = load_credentials(credentials_fp = 'C:/Users/Administrator/creds/creds_people.yml')
with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df_total.to_csv('C:/Users/Administrator/Desktop/output.csv')
import psycopg2
credentials_postgre = credentials['people_write']
m_dbCon = psycopg2.connect(user=credentials_postgre['username'], password=credentials_postgre['password'], host=credentials_postgre['host'] 
,database=credentials_postgre['database'])
curr = m_dbCon.cursor()
curr.execute('truncate table "people"."OPS_PAYROLL_FT"')
curr.close()
m_dbCon.commit()
postgresql_client.write_table(
    df_total, 
    "OPS_PAYROLL_FT", 
    "people", 
    if_exists = 'append' # see the different values that if_exists can take in the method docsting
)


