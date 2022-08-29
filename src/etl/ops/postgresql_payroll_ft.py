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
print(sys.path)
from refresh_token_factorial import get_token

SCHEMA = "people"
TABLE_NAME = "OPS_PAYROLL_FT"

token = get_token()
url = "https:\\api.factorialhr.com\api\v1\payroll\contract_versions"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df = pd.DataFrame(response.json())
df.drop(columns=['has_payroll','salary_frequency','es_has_teleworking_contract', 'es_cotization_group',
'es_contract_observations','es_job_description','es_working_day_type_id',
'es_education_level_id', 'es_professional_category_id',
'fr_employee_type', 'fr_forfait_jours', 'fr_jours_par_an',
'fr_coefficient', 'fr_contract_type_id', 'fr_level_id', 'fr_step_id',
'fr_mutual_id', 'fr_professional_category_id', 'fr_work_type_id',
'de_contract_type_id'], inplace=True)

credentials = load_credentials('C:\Users\Administrator\creds\creds_people.yml')
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
postgresql_client.write_table(
    df, 
    "OPS_PAYROLL_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)



