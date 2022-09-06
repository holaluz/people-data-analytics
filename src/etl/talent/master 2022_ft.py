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

sh = gspread_client.open('Master File_2022')
ws = sh.worksheet("Budget 2022")
list_cols = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35]
df = pd.DataFrame()
for col in list_cols:
    value = ws.col_values(col)
    df_master = pd.DataFrame.from_dict(value)
    df = pd.concat([df, df_master], axis=1)

df.columns = df.iloc[0]
df = df[1:]
df.columns

postgre_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgre_client.write_table(
    df, 
    "OPS_MASTER_FT", 
    "temp", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)



"""#4. Query 2 get every new row into df_master

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
df = []
query_master = 'select * from people."temp"."OPS_MASTER_FT" omf'

for chunk in postgresql_client.make_query(query_master, chunksize=160000):
    df.append(chunk)
df_master = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()

print(df_master)

#Fills staff solar_22 with new information from masterfile table 

spreadsheet = gspread_client.open('staff solar_22')
ws = spreadsheet.worksheet('Current STAFF') 
rows = ws.get_all_records() 
df_worksheet = pd.DataFrame.from_dict(rows)
df_total = pd.concat([df_master, df_worksheet])

#Save it into staff_solar table

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgresql_client.write_table(
    df_total, 
    "OPS_STAFF_SOLAR_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)"""

