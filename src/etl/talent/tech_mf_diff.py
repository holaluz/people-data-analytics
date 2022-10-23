import os 
import gspread
import pandas as pd
import numpy as np
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
df_selection = df_tech.iloc[:,0:14]
df_selection.columns = ['Gender','Ubicación','id','apellidos,nombre','job title','supply solar tech','status,''tipo de contrato','split','new position or backfill','profile','seniority','sociedad','team','subteam']


#df_selection = df_selection.dropna(subset=['id'], inplace=False)

#1.Query activos with new Ids on (df_master)

postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master = """select a."Id", a."Apellidos, Nombre", a."Job title", a."Split", a."Sub Team",
a."Sociedad", row_number() over (ORDER by(select null))as rownum
from temp."OPS_MASTER_FT" a
where a."Status" like '%Activo%' and a."Id" <> '' and a."Id" is not NULL"""

for chunk in postgresql_client.make_query(query_master, chunksize=160000):
    df.append(chunk)
df_master = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()

print(df_master)

#1.Merge both DF(sheets) to find differences

df_master.dropna(subset=['id'], inplace=True)
df_master.rename(columns={'id':'Id'}, inplace=True)
df_master.rename(columns={'sociedad':'Sociedad'}, inplace=True)


df_merge = pd.merge(df_master,df_selection, how='inner', on = ['id', 'sociedad'])

df_merge['differences'] = np.where((df_merge['job title']!=df_merge['Job title']), True, False)

 | 
"""(df_merge['split']!=df_merge['SPLIT']),"""

#1.Select only those we will need and the difference column

cols_diff = df_merge[['Job title','job title','rownumber']]
result_df= cols_diff.loc[df_merge['differences']==True]

#Send message in slack with diff

result_df.to_csv('differences.csv', index = False)

send_file_by_slack('differences.csv','Differences staff solar_22',
credentials['slack_differences_staffsolar'],
"__file__",channel = 'ppl_differences_staffsolar',     
link_names = 1, verbose = True)








