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
query_master_append = """select cast(a."Id" as char(10)), a."Apellidos, Nombre", a."Job title", a."Team", a."Sub Team", a."Split",a."Sociedad",
a."Start date", a."New position or backfill", a."Status", a."Tipo de contrato", a."MANAGER", a."Ubicación" , null as squad , a."End date",
null as tipobaja, null as chapter,
row_number() over (ORDER by(select null))as rownum
from "temp"."OPS_MASTER_FT" a
left join "temp"."TAL_STAFF_SOLAR_FT" b 
on cast(a."Id" as char(10)) = cast(b."Id" as char(10)) and a."Sociedad" = b."Sociedad" where "Supply/Solar/Tech" like '%Solar%'
and cast(b."Id" as char(10)) is null"""""

for chunk in postgresql_client.make_query(query_master_append, chunksize=160000):
    df.append(chunk)
if df != [] :   
    df_master_append = pd.concat(df, ignore_index=True)
else:
    df_master_append = pd.DataFrame()
postgresql_client.close_connection()

#Fills staff solar_22 with new information from masterfile table 
#1.Reads destination spreadsheet
spreadsheet = gspread_client.open('staff solar_22')
ws = spreadsheet.worksheet('Current STAFF') 
rows = ws.get_values() 
df_staff_solar = pd.DataFrame.from_dict(rows)
df_staff_solar['rownumber']=df_staff_solar.index 
df_staff_solar.columns= df_staff_solar.iloc[0,:] #remove numerical headers
df_staff_solar = df_staff_solar.iloc[1:,:]
df_staff_solar.rename(columns={0:'rownumber'}, inplace=True)
df_staff_solar.rename(columns={'Id':'id','Sociedad':'sociedad'}, inplace=True)
df_staff_solar = df_staff_solar.dropna(subset=['id'], inplace=False)

#Fills staff solar_22 with new information from masterfile table 
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
left join temp."TAL_STAFF_SOLAR_FT" b 
on a."Id" = b."Id" and a."Sociedad" = b."Sociedad" 
where "Supply/Solar/Tech" like '%Solar%' and a."End date" not like '%p%'
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

df_merge = pd.merge(df_diff,df_staff_solar, how='inner', on = ['id','sociedad'])
df_merge['end date']=df_merge['end date'].astype(str)
df_merge['differences_date'] = np.where((df_merge['end date']!=df_merge['fecha de baja']), True, False) 
df_merge['differences_status'] = np.where((df_merge['status']!=df_merge['Status']), True, False)
df_merge['differences_split'] = np.where((df_merge['split']!=df_merge['SPLIT']), True, False)
df_merge['differences_team'] = np.where((df_merge['team']!=df_merge['TEAM']), True, False) 
df_merge['differences_subteam'] = np.where((df_merge['sub team']!=df_merge['SUBTEAM']), True, False) 
df_merge['differences_jobtitle'] = np.where((df_merge['job title']!=df_merge['Job title']), True, False) 
df_merge['differences_manager'] = np.where((df_merge['manager']!=df_merge['Manager']), True, False) 
df_merge['differences_salary'] = np.where((df_merge['fix salary']!=df_merge['Fix Salary']), True, False) 
df_merge['differences_bonus'] = np.where((df_merge['bonus']!=df_merge['Bonus']), True, False) 


#1.Select only those we will need and the difference column
cols_diff = df_merge[[
'status','Status','end date','fecha de baja','differences_status','differences_date','Apellidos, Nombre',
'differences_split','split','SPLIT',
'differences_team','team','TEAM',
'differences_subteam','sub team','SUBTEAM',
'differences_jobtitle','job title','Job title', 
'differences_manager','manager','Manager',
'differences_salary','fix salary','Fix Salary',
'differences_bonus','bonus','Bonus','rownumber']]

result_df= cols_diff.loc[df_merge['differences_status']==True]
result_df2= cols_diff.loc[df_merge['differences_date']==True]
result_df3= cols_diff.loc[df_merge['differences_split']==True]
result_df4= cols_diff.loc[df_merge['differences_team']==True]
result_df5= cols_diff.loc[df_merge['differences_subteam']==True]
result_df6= cols_diff.loc[df_merge['differences_jobtitle']==True]
result_df7= cols_diff.loc[df_merge['differences_manager']==True]
result_df8= cols_diff.loc[df_merge['differences_salary']==True]
result_df9= cols_diff.loc[df_merge['differences_bonus']==True]


#Update those columns on destination gsheets
#1.Forloop iteration according to rownumber in the selected changed columns
count=0
for index, row in result_df.iterrows():
    ws.update('J'+str(1+row['rownumber']), [[row['status']]])
    sleep(2)
    print(count)
    count=count+1
count=0
for index, row in result_df2.iterrows():
    ws.update('O'+str(1+row['rownumber']), [[row['end date']]])
    sleep(2)
    print(count)
    count=count+1      
count=0
for index, row in result_df3.iterrows():
    ws.update('F'+str(1+row['rownumber']), [[row['split']]])
    sleep(2)
    print(count)
    count=count+1          
count=0
for index, row in result_df4.iterrows():
    ws.update('E'+str(1+row['rownumber']), [[row['team']]])
    sleep(2)
    print(count)
    count=count+1     
count=0
for index, row in result_df5.iterrows():
    ws.update('D'+str(1+row['rownumber']), [[row['sub team']]])
    sleep(2)
    print(count)
    count=count+1          
count=0
for index, row in result_df6.iterrows():
    ws.update('C'+str(1+row['rownumber']), [[row['job title']]])
    sleep(2)
    print(count)
    count=count+1
count=0
for index, row in result_df7.iterrows():
    ws.update('L'+str(1+row['rownumber']), [[row['manager']]])
    sleep(2)
    print(count)
    count=count+1
count=0
for index, row in result_df8.iterrows():
    ws.update('R'+str(1+row['rownumber']), [[row['fix salary']]])
    sleep(2)
    print(count)
    count=count+1
count=0
for index, row in result_df9.iterrows():
    ws.update('S'+str(1+row['rownumber']), [[row['bonus']]])
    sleep(2)
    print(count)
    count=count+1

