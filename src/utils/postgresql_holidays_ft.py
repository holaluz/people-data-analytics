# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 17:06:30 2022

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
url = "https://api.factorialhr.com/api/v1/time/leaves"

headers = {'Authorization': f'Bearer {token}'}
response = requests.request("GET", url, headers=headers)

df = pd.DataFrame(response.json())
columns = list(df.columns)

with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
postgresql_client.write_table(
    df, 
    "PPL_HOLIDAYS_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)
