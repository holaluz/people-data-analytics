import os 
import gspread
import pandas as pd
import yaml
from genericpath import exists
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.credentials import load_google_drive_service_account_credentials

#1.Request access to the google sheet with json credentials

LOCAL_CREDS_PATH = os.path.join(os.environ['USERPROFILE'], 'creds')
DRIVE_CREDS_FILENAME = 'drive_to_python.json'

sheet_credentials = load_google_drive_service_account_credentials(
    credentials_fp=os.path.join(LOCAL_CREDS_PATH, DRIVE_CREDS_FILENAME)
)

gspread_client = gspread.authorize(sheet_credentials)

sh = gspread_client.open('Corporate_Master File_2022')
ws = sh.worksheet("Budget 2022")
rows = ws.get_values() 
df_ws = pd.DataFrame.from_dict(rows)
df = df_ws.iloc[:,0:25]
df.columns= df.iloc[0,:] #remove numerical headers
df = df.iloc[1:,:]

with open(os.path.join('C:/Users/Administrator/creds', 'creds_people.yml')) as file:
    credentials = yaml.load(file, Loader=yaml.FullLoader)
postgre_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
postgre_client.write_table(
    df, 
    "TAL_CORPORATE_FT", 
    "temp", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)
