import os 
import gspread
import pandas as pd
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

sh = gspread_client.open('staff solar_22')
ws = sh.worksheet("Current STAFF")
list_cols = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]
list_cols = ws.get_all_records() 
df = pd.DataFrame()
for col in list_cols:
    value = ws.col_values(col)
    df_master = pd.DataFrame.from_dict(value)
    df = pd.concat([df, df_master], axis=1)

df.columns = df.iloc[0]
df = df[1:]
df.columns
print(df)

#df_test = df_worksheet['Apellidos, Nombre'].str.split(',', expand=True)
#df_test.rename(columns = {0: 'last_name', 1:'first_name'}, inplace=True)
#last_name_ls = df_test.pop('last_name')
#df_worksheet.insert(2,'last_name', last_name_ls)
#first_name_ls = df_test.pop('first_name')
#df_worksheet.insert(3,'first_name', first_name_ls)

postgre_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgre_client.write_table(
    df, 
    "TAL_STAFF_SOLAR_FT", 
    "temp", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)
