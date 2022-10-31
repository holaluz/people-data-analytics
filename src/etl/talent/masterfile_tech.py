import os 
import sys
import gspread
import yaml
import pandas as pd
#import time
from time import sleep
from genericpath import exists
import numpy as np
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.credentials import load_google_drive_service_account_credentials
#sys.path.append(os.path.join(os.environ['USERPROFILE'], 'documents', 'github', 'people-data-analytics', 'src', 'utils'))
#print(sys.path)
#1.Request access to the google sheet with json credentials
LOCAL_CREDS_PATH = os.path.join(os.environ['USERPROFILE'], 'creds')
DRIVE_CREDS_FILENAME = 'drive_to_python.json'
CREDS_FILENAME = 'creds_people.yml'
credentials= load_credentials(credentials_fp=os.path.join(LOCAL_CREDS_PATH, CREDS_FILENAME))
sheet_credentials = load_google_drive_service_account_credentials(
    credentials_fp=os.path.join(LOCAL_CREDS_PATH, DRIVE_CREDS_FILENAME)
)
gspread_client = gspread.authorize(sheet_credentials)
#4. Query 2 get every new row from df_master and append it
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master_append = """select a."Gender", a."Ubicación", a."Id", a."Apellidos, Nombre", a."Job title", 
a."Supply/Solar/Tech", a."Split", a."Sociedad", a."Status", a."Tipo de contrato", a."New position or backfill", a."Profile",
a."Seniority", a."Team", a."Sub Team", a."CECO Num", a."CECO FINANZAS", a."MANAGER", null as "Remote", a."Start date", a."End date", a."FTE según jornada",
a."Jornada (%)", a."Fix Salary", a."Bonus", a."Total (Salary + Bonus)", row_number() over (ORDER by(select null))as rownum
from "temp"."OPS_MASTER_FT" a
left join "temp"."TAL_TECH_FT" b
on a."Apellidos, Nombre" = b."Apellidos, Nombre" 
and a."Sociedad" = b."Sociedad" where a."Supply/Solar/Tech" like '%Technology%' 
and b."Id" is null and a."Status" like '%Activo%' or a."Status" like '%Join%'
and a."Supply/Solar/Tech" like '%Technology%'  """""

for chunk in postgresql_client.make_query(query_master_append, chunksize=160000):
    df.append(chunk)
if df != [] :   
    df_master_append = pd.concat(df, ignore_index=True)
else:
    df_master_append = pd.DataFrame()
postgresql_client.close_connection()

#Fills Tech_Master File_2022 with new information from masterfile table 
#1.Reads destination spreadsheet
spreadsheet = gspread_client.open('Tech_Master File_2022')
ws = spreadsheet.worksheet('Budget 2022') 
rows = ws.get_values() 
df_tech = pd.DataFrame.from_dict(rows)
df_tech['rownumber']=df_tech.index 
df_tech.columns= df_tech.iloc[0,:] #remove numerical headers
df_tech = df_tech.iloc[1:,:]
df_tech.rename(columns={0:'rownumber'}, inplace=True)
df_tech.rename(columns={'Id':'id','Sociedad':'sociedad'}, inplace=True)
df_tech = df_tech.dropna(subset=['id'], inplace=False)

#Fills Tech_Master File_2022 with new information from masterfile table 
#Append new rows

df_total = ws.append_rows(df_master_append.values.tolist(), table_range='A1')

#4. Query 2 get latest start_date contract to update end date and status at staff_solar
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master_update = """select a."Apellidos, Nombre", a."Id", a."Sociedad", min(a."Status") as Status, case when max(to_date(case when a."End date" = '' then '01/01/2040' else a."End date" end, 'DD/MM/YYYY')) = 
to_date('01/01/2040', 'DD/MM/YYYY') then null else max(to_date(case when a."End date" = '' then '01/01/2040' else a."End date" end, 'DD/MM/YYYY')) end as "End date", max(a."Fix Salary") as "Fix Salary",
max(a."Bonus") as "Bonus", a."Job title",a."Split", a."Team", a."Sub Team", a."MANAGER"
from (select a."Id", max(a."Start date")as latest_start_date, a."Sociedad"
from temp."OPS_MASTER_FT" a 
left join temp."TAL_TECH_FT" b 
on a."Id" = b."Id" and a."Sociedad" = b."Sociedad" 
where a."Supply/Solar/Tech" like '%Technology%' and a."End date" not like '%p%' and a."End date" not like '%TB%'
group by a."Id", a."Sociedad")f
inner join (select * from temp."OPS_MASTER_FT")a
on  a."Id"= f."Id" and f.latest_start_date = a."Start date" and a."Sociedad"= f."Sociedad"
group by a."Id", a."Apellidos, Nombre", a."Sociedad",a."Job title",a."Split", a."Team", a."Sub Team", a."MANAGER"
"""""
for chunk in postgresql_client.make_query(query_master_update, chunksize=160000):
    df.append(chunk)
df_diff = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()
print(df_diff)

#Update values

#1.Merge both DF(sheets) to find differences

df_merge = pd.merge(df_diff,df_tech, how='inner', on = ['id','sociedad'])
df_merge['end date']=df_merge['end date'].astype(str)
df_merge['differences_date'] = np.where((df_merge['end date']!=df_merge['End date']), True, False) 
df_merge['differences_status'] = np.where((df_merge['status']!=df_merge['Status']), True, False)
df_merge['differences_salary'] = np.where((df_merge['fix salary']!=df_merge['Fix Salary']), True, False) 
df_merge['differences_bonus'] = np.where((df_merge['bonus']!=df_merge['Bonus']), True, False) 


#1.Select only those we will need and the difference column
cols_diff = df_merge[['status','Status','end date','End date','differences_status','differences_date',
'differences_salary','fix salary','Fix Salary',
'differences_bonus','bonus','Bonus','rownumber']]

result_df= cols_diff.loc[df_merge['differences_status']==True]
result_df1= cols_diff.loc[df_merge['differences_date']==True]
result_df2= cols_diff.loc[df_merge['differences_salary']==True]
result_df3= cols_diff.loc[df_merge['differences_bonus']==True]


#Update those columns on destination gsheets
#1.Forloop iteration according to rownumber in the selected changed columns
count=0
for index, row in result_df.iterrows():
    ws.update('I'+str(1+row['rownumber']), [[row['status']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df1.iterrows():
    ws.update('T'+str(1+row['rownumber']), [[row['end date']]])
    sleep(3)
    print(count)
    count=count+1      
count=0
for index, row in result_df2.iterrows():
    ws.update('W'+str(1+row['rownumber']), [[row['fix salary']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df3.iterrows():
    ws.update('X'+str(1+row['rownumber']), [[row['bonus']]])
    sleep(3)
    print(count)
    count=count+1

