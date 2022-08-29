from datetime import datetime
import pandas as pd
import http.client
import json
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
credentials = load_credentials('C:/Users/Administrator/creds/creds_people.yml')

def refresh_api(refresh_token):
    conn = http.client.HTTPSConnection("api.factorialhr.com")
    payload = 'client_id=b7wlIaFemFfQ7-xogFQMKEbf4ndzk7M9oT86NKSgSaE&client_secret=o1DPbdx9czt4u99EOcZW9yy5v_3DhR6Giut2bQPYieI&refresh_token='+refresh_token+'&grant_type=refresh_token'
    conn.request("POST", "/oauth/token", payload)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    data = json.loads(data.decode("utf-8"))
    return data['access_token'], data['refresh_token']


def get_last_token():
    pgsql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
    query_get_token = """select * from people."temp"."LOG_TOKEN_FACTORIAL" ltf"""
    df_token = pgsql_client.make_query(query_get_token)
    lt_ls = list(df_token['refresh_token'])
    last_token = lt_ls[0]
    return last_token 


def write_actual_token(refresh_token): 
    pgsql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
    data={'refresh_token':[refresh_token],'date':[datetime.today().strftime('%Y%m%d')]}
    df=pd.DataFrame(data)
    pgsql_client.write_table(
    df, 
    "LOG_TOKEN_FACTORIAL", 
    "temp", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
    )
   

def get_token(): 
    last_token = get_last_token()
    access_token, refresh_token = refresh_api(last_token)
    write_actual_token(refresh_token)
    return access_token





















