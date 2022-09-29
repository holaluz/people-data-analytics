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

sh = gspread_client.open('staffsolartest')
ws = sh.worksheet("Current STAFF")
rows = ws.get_values() 
df_worksheet = pd.DataFrame.from_dict(rows)
df_selection = df_worksheet.iloc[:,0:8]
df_selection.columns = ['id', 'apellidos,nombre','job title', 'sub team', 'team', 'split', 'sociedad', 'fecha de baja']
df_selection['rownumber']=df_selection.index #Create rownumber column for the forloop 
df_selection.columns= df_selection.iloc[0,:] #remove numerical headers
df_selection = df_selection.iloc[1:,:]
#df_selection = df_selection.dropna(subset=['id'], inplace=False)

#1.Query activos with new Ids on (df_master)

postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
df = []
query_master = """select a."Id", a."Apellidos, Nombre", a."Job title", a."Sub Team", a."Team", a."Split", 
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


df_merge = pd.merge(df_master,df_selection, how='inner', on = ['Id', 'Sociedad'])

df_merge['differences'] = np.where((df_merge['job title']!=df_merge['Job title']) | 
(df_merge['sub team']!=df_merge['SUBTEAM']) | 
(df_merge['team']!=df_merge['TEAM']) | 
(df_merge['split']!=df_merge['SPLIT']), True, False)


#1.Select only those we will need and the difference column

cols_diff = df_merge[['Job title','job title','sub team','SUBTEAM','team','TEAM', 'split','SPLIT','differences_jobtitle','differences_subteam','differences_team','differences_split','rownumber']]
result_df= cols_diff.loc[df_merge['differences']==True]

#Send message in slack with diff

#buffer=io.BytesIO()
result_df.to_csv('differences.csv', index = False)
#buffer.seek(0)

send_file_by_slack('differences.csv','Differences staff solar_22',
credentials['slack_differences_staffsolar'],
"__file__",channel = 'ppl_differences_staffsolar',     
link_names = 1, verbose = True)

#1.Example for loop

#for index, row in result_df.iterrows():
    #ws.update(result_df, [[42], [43]]) Update information in a spreadsheet
    #k=row

#result_df=result_df[0:2]






