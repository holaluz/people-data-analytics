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
url = "https://api.factorialhr.com/api/v1//custom_fields/values"

headers = {'Authorization': f'Bearer {token}'}
params = {'lang':'en','tag':'python'}
response = requests.request("GET", url, headers=headers, params=params)

df = pd.DataFrame(response.json())
columns = list(df.columns)

print(df)