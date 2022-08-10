import requests
import os 
import sys
import json
sys.path.append(os.path.realpath(os.path.join(os.dirname(__file__), '..','..','utils')))
sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
print(sys.path)
from refresh_token_factorial import get_token
json.loads(response.decode('utf-8'))[0]


token = get_token()
url = "https://api.factorialhr.com/api/v1/payroll/contract_versions"

payload={}
headers = {'Authorization': f'Bearer {token}'}
print(headers)

response = requests.request("GET", url, headers=headers, data=payload)
response_text = response.json()

response.content

from datetime import date
import pandas as pd
df = pd.DataFrame(json.loads(response.text))
df.columns
df.drop(columns=['has_payroll','salary_frequency','es_has_teleworking_contract', 'es_cotization_group',
'es_contract_observations','es_job_description','es_working_day_type_id',
'es_education_level_id', 'es_professional_category_id',
'fr_employee_type', 'fr_forfait_jours', 'fr_jours_par_an',
'fr_coefficient', 'fr_contract_type_id', 'fr_level_id', 'fr_step_id',
'fr_mutual_id', 'fr_professional_category_id', 'fr_work_type_id',
'de_contract_type_id'], inplace=True)

from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgresql_client.write_table(
    df, 
    "OPS_PAYROLL_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)

#df = pd.DataFrame.from_dict(data, orient='index')
df.head()


