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
query_master_append = """select a."Gender", a."Ubicación", a."Id", a."Id Req > DNI/NIE", a."Apellidos, Nombre", a."Job title", a."Supply/Solar/Tech", a."Split",
a."Sociedad", a."Status", a."Tipo de contrato", a."New position or backfill", a."Profile", a."Seniority", a."Q",
a."Team",a."Sub Team", a."CECO Num" , a."CECO FINANZAS", a."MANAGER", a."Start date", a."End date", a."Fecha del cambio", a."31/12/2022", 
a."FTE según jornada",a."FTE según fecha alta + jornada", a."Jornada (%)", a."Fix Salary", a."Bonus", a."Dietas/Guardias centro control", a."KM", a."TOTAL FIX + Bonus", row_number() over (ORDER by(select null))as rownum
from "temp"."OPS_MASTER_FT" a
left join "temp"."TAL_STAFF_SOLAR_FT" b 
on a."Apellidos, Nombre" = b."Apellidos, Nombre" 
where a."Supply/Solar/Tech" like '%Supply%' and a."Team" = 'Sales'
and b."Apellidos, Nombre" is null and (a."Status" like '%Activo%' or a."Status" like '%Join%')
and a."Sociedad" like '%Holaluz Clidom%'
"""

for chunk in postgresql_client.make_query(query_master_append, chunksize=160000):
    df.append(chunk)
if df != [] :   
    df_master_append = pd.concat(df, ignore_index=True)
else:
    df_master_append = pd.DataFrame()
postgresql_client.close_connection()

#Fills staff solar_22 with new information from masterfile table 
#1.Reads destination spreadsheet
spreadsheet = gspread_client.open('Solar_Master File_2022')
ws = spreadsheet.worksheet('Staff Sales 2022') 
rows = ws.get_values() 
df_staff_sales = pd.DataFrame.from_dict(rows)
df_staff_sales['rownumber']=df_staff_sales.index 
df_staff_sales.columns= df_staff_sales.iloc[0,:] #remove numerical headers
df_staff_sales = df_staff_sales.iloc[1:,:]
df_staff_sales.rename(columns={0:'rownumber'}, inplace=True)
df_staff_sales.rename(columns={'Id':'id','Sociedad':'sociedad'}, inplace=True)
df_staff_sales = df_staff_sales.dropna(subset=['id'], inplace=False)

#Fills staff solar_22 with new information from masterfile table 
#Append new rows

df_total = ws.append_rows(df_master_append.values.tolist(), table_range='A1')

#4. Query 2 get latest start_date contract to update end date and status at staff_solar
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master_update = """
select a."Apellidos, Nombre", a."Id", a."Sociedad",  min(a."Status") as Status, 
case when max(to_date(case when a."End date" = '' then '01/01/2040' else a."End date" end, 'DD/MM/YYYY')) = 
to_date('01/01/2040', 'DD/MM/YYYY') 
then null else max(to_date(case when a."End date" = '' then '01/01/2040' else a."End date" end, 'DD/MM/YYYY'))
end as "End date", max(a."Fix Salary") as "Fix Salary", a."Profile", a."Seniority",
max(a."Bonus") as "Bonus", a."Job title",a."Split", a."Team", a."Sub Team", a."MANAGER"
from (select a."Id", max(a."Start date")as latest_start_date, a."Sociedad"
from temp."OPS_MASTER_FT" a 
left join temp."TAL_STAFF_SOLAR_FT" b 
on a."Id" = b."Id" and a."Sociedad" = b."Sociedad" 
where a."Supply/Solar/Tech" like '%Supply%' and a."End date" not like '%pe%' and a."End date" not like '%20%'
and a."Team" = 'Sales'
group by a."Id", a."Sociedad")f
inner join (select * from temp."OPS_MASTER_FT")a
on  a."Id"= f."Id" and f.latest_start_date = a."Start date" and a."Sociedad"= f."Sociedad"
group by a."Id", a."Apellidos, Nombre", a."Sociedad",a."Job title",a."Split", a."Team", a."Sub Team", a."MANAGER",a."Profile", a."Seniority"
"""""
for chunk in postgresql_client.make_query(query_master_update, chunksize=160000):
    df.append(chunk)
df_diff = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()
print(df_diff)


#Update values

#1.Merge both DF(sheets) to find differences

df_merge = pd.merge(df_diff,df_staff_sales, how='inner', on = ['id','sociedad'])
df_merge['end date']=df_merge['end date'].astype(str)
df_merge['differences_date'] = np.where((df_merge['end date']!=df_merge['End date']), True, False) 
df_merge['differences_status'] = np.where((df_merge['status']!=df_merge['Status']), True, False)
df_merge['differences_split'] = np.where((df_merge['split']!=df_merge['Split']), True, False)
df_merge['differences_team'] = np.where((df_merge['team']!=df_merge['Team']), True, False) 
df_merge['differences_subteam'] = np.where((df_merge['sub team']!=df_merge['Sub Team']), True, False) 
df_merge['differences_jobtitle'] = np.where((df_merge['job title']!=df_merge['Job title']), True, False) 
df_merge['differences_manager'] = np.where((df_merge['manager']!=df_merge['MANAGER']), True, False) 
df_merge['differences_salary'] = np.where((df_merge['fix salary']!=df_merge['Fix Salary']), True, False) 
df_merge['differences_bonus'] = np.where((df_merge['bonus']!=df_merge['Bonus']), True, False) 
df_merge['differences_profile'] = np.where((df_merge['profile']!=df_merge['Profile']), True, False) 
df_merge['differences_seniority'] = np.where((df_merge['seniority']!=df_merge['Seniority']), True, False) 




#1.Select only those we will need and the difference column
cols_diff = df_merge[[
'status','Status','end date','End date','differences_status','differences_date','Apellidos, Nombre',
'differences_split','split','Split',
'differences_team','team','Team',
'differences_subteam','sub team','Sub Team',
'differences_jobtitle','job title','Job title', 
'differences_manager','manager','MANAGER',
'differences_salary','fix salary','Fix Salary',
'differences_bonus','bonus','Bonus',
'differences_profile','profile','Profile',
'differences_seniority','seniority','Seniority','rownumber']]

result_df= cols_diff.loc[df_merge['differences_status']==True]
result_df2= cols_diff.loc[df_merge['differences_date']==True]
result_df3= cols_diff.loc[df_merge['differences_split']==True]
result_df4= cols_diff.loc[df_merge['differences_team']==True]
result_df5= cols_diff.loc[df_merge['differences_subteam']==True]
result_df6= cols_diff.loc[df_merge['differences_jobtitle']==True]
result_df7= cols_diff.loc[df_merge['differences_manager']==True]
result_df8= cols_diff.loc[df_merge['differences_salary']==True]
result_df9= cols_diff.loc[df_merge['differences_bonus']==True]
result_df10= cols_diff.loc[df_merge['differences_profile']==True]
result_df11= cols_diff.loc[df_merge['differences_seniority']==True]



#Update those columns on destination gsheets
#1.Forloop iteration according to rownumber in the selected changed columns
count=0
for index, row in result_df.iterrows():
    ws.update('J'+str(1+row['rownumber']), [[row['status']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df2.iterrows():
    ws.update('V'+str(1+row['rownumber']), [[row['end date']]])
    sleep(3)
    print(count)
    count=count+1      
count=0
for index, row in result_df3.iterrows():
    ws.update('H'+str(1+row['rownumber']), [[row['split']]])
    sleep(3)
    print(count)
    count=count+1          
count=0
for index, row in result_df4.iterrows():
    ws.update('P'+str(1+row['rownumber']), [[row['team']]])
    sleep(3)
    print(count)
    count=count+1     
count=0
for index, row in result_df5.iterrows():
    ws.update('Q'+str(1+row['rownumber']), [[row['sub team']]])
    sleep(3)
    print(count)
    count=count+1          
count=0
for index, row in result_df6.iterrows():
    ws.update('F'+str(1+row['rownumber']), [[row['job title']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df7.iterrows():
    ws.update('T'+str(1+row['rownumber']), [[row['manager']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df8.iterrows():
    ws.update('Y'+str(1+row['rownumber']), [[row['fix salary']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df9.iterrows():
    ws.update('Z'+str(1+row['rownumber']), [[row['bonus']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df10.iterrows():
    ws.update('M'+str(1+row['rownumber']), [[row['profile']]])
    sleep(3)
    print(count)
    count=count+1
count=0
for index, row in result_df11.iterrows():
    ws.update('N'+str(1+row['rownumber']), [[row['seniority']]])
    sleep(3)
    print(count)
    count=count+1


