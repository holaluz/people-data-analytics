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

postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
df = []

#4. New query 2 get distinct team_names / Store query_teams into df_teams

query_teams = """select distinct(team_name) from people.people."PPL_EMPLOYEES_FT" 
where team_name is not null"""

for chunk in postgresql_client.make_query(query_teams, chunksize=160000):
    df.append(chunk)
df_teams = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()

#Get team names & store them as a list 

teams_ls = list(df_teams['team_name'])
print(list(df_teams['team_name']))
files_dict = gspread_client.list_spreadsheet_files() #devuelve diccionario
team_names = []

#Forloop that finds any filename starting with chapter_lead 

for n in range(len(files_dict)):
    name = files_dict[n]['name'] #coje la llave name del dict
    print(name)
    if 'chapter_lead_' in name:
        team_names.append(name) 

#Forloop that fills with their team_names       

df_total = pd.DataFrame()
for team in team_names:
    team_gs = team[13::]
    if team_gs in teams_ls: 
        print('t')
        spreadsheet = gspread_client.open(f'{team}')
        ws = spreadsheet.worksheet('Hoja 1') 
        rows = ws.get_values() 
        df_worksheet = pd.DataFrame.from_dict(rows)
        df_total = pd.concat([df_total, df_worksheet],axis=0)
  
df_total.columns= df_total.iloc[0,:] #remove numerical headers
df_total = df_total.iloc[1:,:]

#Save it into pdt table

"""postgresql_client = PostgreSQLClient(**load_credentials('people_write'), lazy_initialization = True)
postgresql_client.write_table(
    df_total, 
    "OPS_PDT_FT", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)"""

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
curr.execute('truncate table "people"."OPS_PDT_FT"')
curr.close()
m_dbCon.commit()
postgresql_client.write_table(
    df_total, 
    "OPS_PDT_FT", 
    "people", 
    if_exists = 'append' # see the different values that if_exists can take in the method docsting
)




