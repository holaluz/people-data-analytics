from genericpath import exists
import os 
import gspread
import pandas as pd
import numpy as np
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.credentials import load_google_drive_service_account_credentials

#***Read from employee & payroll postgres tables & fill the empty rows in the diff created gsheets

#1. Request access to the google sheet with json credentials

LOCAL_CREDS_PATH = os.path.join(os.environ['USERPROFILE'], 'creds')
DRIVE_CREDS_FILENAME = 'drive_to_python.json'
CREDS_FILENAME = 'creds_people.yml'
credentials= load_credentials(credentials_fp=os.path.join(LOCAL_CREDS_PATH, CREDS_FILENAME))

sheet_credentials = load_google_drive_service_account_credentials(
    credentials_fp=os.path.join(LOCAL_CREDS_PATH, DRIVE_CREDS_FILENAME)
)

gspread_client = gspread.authorize(sheet_credentials)

#2. Query the postgres tables with needed rows

postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_contracts = """select distinct a.id, a.first_name, a.last_name, a.email, a.team_name, b.job_title, a."MANAGER",null as profile, a."Sub Team"
from (select a.id,a.first_name, a.last_name,a.email, a.team_name, b."Job title", b."MANAGER",b."Sub Team"
from people.people."PPL_EMPLOYEES_FT" a left join "temp"."OPS_MASTER_FT" b 
on a.factorial_id = b."Id Req > DNI/NIE" 
where a.team_name is not null)a
left join (select c.actual_date, c.employee_id, c.job_title,
RANK () OVER (
		PARTITION BY c.employee_id
		ORDER BY c.actual_date desc
	) rn
from (select max(b.effective_on)as actual_date, b.employee_id, b.job_title from people.people."OPS_PAYROLL_FT" b
group by b.employee_id, b.job_title
order by actual_date desc)c ) b on a.id = b.employee_id 
where b.rn = 1"""

#3. Store query into a dataframe

for chunk in postgresql_client.make_query(query_contracts, chunksize=160000):
    df.append(chunk)
df_contracts = pd.concat(df, ignore_index=True)
df = []

#4. New query 2 get distinct team_names / Store it into a new dataframe (df_teams)

query_teams = """select distinct team_name from people.people."PPL_EMPLOYEES_FT" pef  
where team_name is not null"""


for chunk in postgresql_client.make_query(query_teams, chunksize=160000):
    df.append(chunk)
df_teams = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()

#5. Get team names

teams_ls = list(df_teams['team_name'])
print(list(df_teams['team_name']))
files_dict = gspread_client.list_spreadsheet_files()
team_names = []

#6.Forloop that finds any filename starting with chapter_lead 

for n in range(len(files_dict)):
    name = files_dict[n]['name']
    print(name)
    if 'chapter_lead_' in name:
        team_names.append(name)

#7.Forloop that fills with their team_names       

for team in team_names:
    team_gs = team[13::]
    if team_gs in teams_ls: 
        df_contracts_team = df_contracts.query(f"team_name == '{team_gs}'")
        print(df_contracts_team)
        spreadsheet = gspread_client.open(f'{team}')
        ws = spreadsheet.worksheet('Hoja 1') 
        rows = ws.get_all_records() 
        #len_rows = len(rows)
        #if len_rows >= 2: 
            #ws.delete_rows(start_index = 2, end_index = len_rows + 1 )
    ws.append_rows(df_contracts_team.values.tolist(), table_range='A:I')
    ws.format('A:G', {'textFormat': {'bold': False}})
    ws.format('A1:Z1', {'textFormat': {'bold': True}})

