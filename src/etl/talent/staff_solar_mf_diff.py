import os 
import gspread
import pandas as pd
import numpy as np
import io
from genericpath import exists
from holaluz_datatools.slack import send_message_by_slack
from holaluz_datatools.slack import send_file_by_slack

from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
from holaluz_datatools.credentials import load_google_drive_service_account_credentials

#1.Request access to the google sheet with json credentials

LOCAL_CREDS_PATH = os.path.join(os.environ['USERPROFILE'], 'creds')
DRIVE_CREDS_FILENAME = 'drive_to_python.json'
CREDS_FILENAME = 'creds_people.yml'
credentials= load_credentials(credentials_fp=os.path.join(LOCAL_CREDS_PATH, CREDS_FILENAME))
sheet_credentials = load_google_drive_service_account_credentials(
    credentials_fp=os.path.join(LOCAL_CREDS_PATH, DRIVE_CREDS_FILENAME)
)

gspread_client = gspread.authorize(sheet_credentials)

#1.Read origin spreadsheet / Select just columns that need to be transferred 

sh = gspread_client.open('Solar_Master File_2022')
ws = sh.worksheet("Budget 2022")
rows = ws.get_values() 
df_worksheet = pd.DataFrame.from_dict(rows)
df_selection = df_worksheet.iloc[:,0:26]
df_selection.columns = ['Gender', 'Ubicación', 'id', 'Apellidos, Nombre', 'Job title',
       'Supply/Solar/Tech', 'Split', 'sociedad', 'Status', 'Tipo de contrato',
       'New position or backfill', 'Profile', 'Seniority', 'Team', 'Sub Team',
       'CECO Num', 'CECO FINANZAS', 'MANAGER', 'Start date', 'End date',
       'FTE según jornada', 'Jornada (%)', 'Fix Salary', 'Bonus',
       'Total (Salary + Bonus)', 'rownumber']
df_selection.columns= df_selection.iloc[0,:] #remove numerical headers
df_selection = df_selection.iloc[1:,:]
df_selection['rownumber']=df_selection.index #Create rownumber column for the forloop 

df_selection = df_selection.loc[df_selection['Status'] == '00 - Activo']

#df_selection = df_selection.dropna(subset=['id'], inplace=False)

#1.Query activos with new Ids on (df_master)

postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master = """select a."Id", a."Apellidos, Nombre", a."Job title", a."Sub Team", a."Team", a."Supply/Solar/Tech",
a."Sociedad", a."Status", a."Tipo de contrato", a."MANAGER", a."Profile", a."Seniority", row_number() over (ORDER by(select null))as rownum
from temp."OPS_MASTER_FT" a
where a."Status" like '%Activo%' and a."Id" <> '' and a."Id" is not NULL
and a."Supply/Solar/Tech" like '%Solar%' """

for chunk in postgresql_client.make_query(query_master, chunksize=160000):
    df.append(chunk)
df_master = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()


#1.Merge both DF(sheets) to find differences


df_master.rename(columns={'id':'Id'}, inplace=True)
df_master.rename(columns={'sociedad':'Sociedad'}, inplace=True)
df_selection.rename(columns={'id':'Id'}, inplace=True)
df_selection.rename(columns={'sociedad':'Sociedad'}, inplace=True)
df_master.rename(columns={'apellidos, nombre':'Apellidos, Nombre'}, inplace=True)
df_selection.rename(columns={'apellidos, nombre':'Apellidos, Nombre'}, inplace=True)


df_merge = pd.merge(df_master,df_selection, how='inner', on = ['Apellidos, Nombre'])

df_merge['differences'] = np.where((df_merge['job title']!=df_merge['Job title']) | 
(df_merge['sub team']!=df_merge['Sub Team']) | 
(df_merge['team']!=df_merge['Team']) | 
(df_merge['status']!=df_merge['Status']) | 
(df_merge['tipo de contrato']!=df_merge['Tipo de contrato']) | 
(df_merge['manager']!=df_merge['MANAGER']) | 
(df_merge['profile']!=df_merge['Profile']) | 
(df_merge['seniority']!=df_merge['Seniority']) | 
(df_merge['supply/solar/tech']!=df_merge['Supply/Solar/Tech']), True, False)

df_merge= df_merge.rename(columns={'job title':'job_title_master', 'Job title':'job_title_solar' })
df_merge= df_merge.rename(columns={'sub team':'sub_team_master', 'Sub Team':'sub_team_solar' })
df_merge= df_merge.rename(columns={'team':'team_master', 'Team':'team_solar' })
df_merge= df_merge.rename(columns={'supply/solar/tech':'supply/solar/tech_master', 'Supply/Solar/Tech':'supply/solar/tech_solar' })
df_merge= df_merge.rename(columns={'status':'status_master', 'Status':'status_solar' })
df_merge= df_merge.rename(columns={'manager':'manager_master', 'MANAGER':'manager_solar' })
df_merge= df_merge.rename(columns={'profile':'profile_master', 'Profile':'profile_solar' })
df_merge= df_merge.rename(columns={'seniority':'seniority_master', 'Seniority':'seniority_solar' })
df_merge= df_merge.rename(columns={'tipo de contrato':'tipo_contrato_master', 'Tipo de contrato':'tipo_contrato_solar' })


#1.Select only those we will need and the difference column

cols_diff = df_merge[['Apellidos, Nombre','job_title_master','job_title_solar','team_master','team_solar','sub_team_master','sub_team_solar','supply/solar/tech_master', 'supply/solar/tech_solar','status_master','status_solar','tipo_contrato_master', 'tipo_contrato_solar','profile_master', 'profile_solar','seniority_master', 'seniority_solar','manager_master', 'manager_solar','rownumber']]

result_df= cols_diff.loc[df_merge['differences']==True]

#Send message in slack with diff

#buffer=io.BytesIO()
result_df.to_csv('differences_solar.csv', index = False)
#buffer.seek(0)

send_file_by_slack('differences_solar.csv','Differences staff solar_22',
credentials['slack_differences_staffsolar'],
"__file__",channel = 'ppl_differences_staff',     
link_names = 1, verbose = True)

#1.Example for loop

#for index, row in result_df.iterrows():
    #ws.update(result_df, [[42], [43]]) Update information in a spreadsheet
    #k=row

#result_df=result_df[0:2]






