import os 
import sys
import gspread
import yaml
import pandas as pd
from genericpath import exists
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.credentials import load_google_drive_service_account_credentials

#sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
#print(sys.path)

#1.Request access to the google sheet with json credentials

LOCAL_CREDS_PATH = os.path.join(os.environ['USERPROFILE'], 'creds')
DRIVE_CREDS_FILENAME = 'drive_to_python.json'

sheet_credentials = load_google_drive_service_account_credentials(
    credentials_fp=os.path.join(LOCAL_CREDS_PATH, DRIVE_CREDS_FILENAME)
)

gspread_client = gspread.authorize(sheet_credentials)

sh = gspread_client.open('Copia de Master File_2022')
ws = sh.worksheet("Budget 2022")
list_cols = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35]
df = pd.DataFrame()
for col in list_cols:
    value = ws.col_values(col)
    df_master = pd.DataFrame.from_dict(value)
    df = pd.concat([df, df_master], axis=1)

"""sh = gspread_client.open('Copia de Master File_2022')
ws = sh.worksheet("Budget 2022")
rows = ws.get_all_records() 
df = pd.DataFrame.from_dict(rows)"""

df.columns = df.iloc[0]
df = df[1:]
df.columns
print(df)

with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgre_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
postgre_client.write_table(
    df, 
    "OPS_MASTER_FT", 
    "temp", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)

