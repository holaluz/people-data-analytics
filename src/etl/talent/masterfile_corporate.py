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
query_master_append = """
with master as (
select a."Gender", a."Ubicación", a."Id", a."Id Req > DNI/NIE", a."Apellidos, Nombre",
a."Job title", a."Supply/Solar/Tech", a."Split",
a."Sociedad", a."Status", a."Tipo de contrato", a."New position or backfill", a."Profile", a."Seniority", a."Q",
a."Team",a."Sub Team", a."CECO Num" , a."CECO FINANZAS", a."MANAGER", a."Start date", a."End date", 
a."FTE según jornada", a."Jornada (%)", a."Fix Salary", a."Bonus", a."Total (Salary + Bonus)",
row_number() over (partition by a."Apellidos, Nombre" ORDER by rownum_file_master)as rownum
from (select *,
row_number() over (ORDER by(select null)) as rownum_file_master from
"temp"."OPS_MASTER_FT") a
where a."Supply/Solar/Tech" like '%Supply%'
and a."Team"  not like '%Sales%' and a."Job title" not like '%Ventas%'
and a."Team"  not like '%People%' and a."Job title" not like '%Founder%'and a."Job title" not like '%Talent%' and a."End date" not like '%p%' and a."End date" not like '%TB%'
 and a."Apellidos, Nombre" is not null and a."Apellidos, Nombre"<> ''),
talent as (
select "Apellidos, Nombre" as name_tl,
"Status" as status_tl,
"End date" as end_date_tl,
"Split" as split_tl,
"Team" as team_tl,
"Sub Team" as subteam_tl,
"Job title" as job_title_tl,
"MANAGER" as manager_tl,
"Fix Salary" as fix_salary_tl,
"Bonus" as bonus_tl,
"Profile" as profile_tl,
"Seniority" as seniority_tl,
row_number() over (partition by b."Apellidos, Nombre" ORDER by rownum_file ) as rownum_tl,
rownum_file
from (
select *,
row_number() over (ORDER by(select null)) as rownum_file from
temp."TAL_CORPORATE_FT")  b  )
select
case when rownum_tl is null then 1 else 0 end as insert_,
case when job_title_tl <> "Job title" or "Split" <> split_tl or "Status" <> status_tl then 1 else 0 end as update_1,
case when profile_tl <> "Profile" or seniority_tl <> "Seniority" or team_tl <> "Team" or subteam_tl <> "Sub Team" then 1 else 0 end as update_2,
case when manager_tl <> "MANAGER" then 1 else 0 end as update_manager,
case when "End date" <> end_date_tl then 1 else 0 end as update_end_date,
case when "Fix Salary" <> fix_salary_tl or "Bonus" <> bonus_tl then 1 else 0 end as update_salaries,
rownum_file,
"Gender",
"Ubicación", "Id", "Apellidos, Nombre", "Job title", "Supply/Solar/Tech", "Split", "Q",
"Sociedad", "Status", "Tipo de contrato", "New position or backfill", "Profile", "Seniority",
"Team","Sub Team", "CECO Num" , "CECO FINANZAS", "MANAGER", "Start date", "End date",
"FTE según jornada","Jornada (%)", "Fix Salary", "Bonus", "Total (Salary + Bonus)"
from master
left join
talent on master."Apellidos, Nombre"=talent.name_tl
and master.rownum=talent.rownum_tl


"""


for chunk in postgresql_client.make_query(query_master_append, chunksize=160000):
    df.append(chunk)
df_main = pd.concat(df, ignore_index=True)
postgresql_client.close_connection()


# Splits between updates and append

insert_df = df_main[df_main['insert_'] == 1]
update_df1 = df_main[df_main['update_1'] == 1]
update_df2 = df_main[df_main['update_2'] == 1]
update_df_manager = df_main[df_main['update_manager'] == 1]
update_df_end_date = df_main[df_main['update_end_date'] == 1]
update_df_salaries = df_main[df_main['update_salaries'] == 1]

# Init spreadsheet
spreadsheet = gspread_client.open('Solar_Master File_2022')
ws = spreadsheet.worksheet('Budget 2022') 

# inserts rows in worksheet
if df != [] :   
    df_append = insert_df.iloc[:,7:]
    df_total = ws.append_rows(df_append.values.tolist(), table_range='A1')

# Update

def update_fields(ws, df, ini_sheet_col, end_sheet_col, rowcol_name = 'rownum_file', fields_to_updt = [], skip_fields=0):
    """
    Updates the fields of a worksheet given the source and destination cols.
    - ws: the worksheet to update
    - df: the dataframe containing the values of the fields to update
    - ini_sheet_col: the column letter of the range start of the update
    - end_sheet_col: the column letter of the range end of the update
    - rowcol_name: the name of the column of the dataframe df which contains the value of the row number of the destination row to update
    - fields_to_updt: list of fields of df to use to update (order by destination sequence)
    - skip_fields: to discard the first n columns of the dataframe
    """
    assert len(fields_to_updt) > 0 

    update_df = df.iloc[:, skip_fields:]
    
    for index, row in update_df.iterrows():
        row_num = str(1+int(row[rowcol_name]))
        ws.update(ini_sheet_col + row_num +':'+ end_sheet_col + row_num, [[row[name] for name in fields_to_updt]])
        sleep(3)

update_fields(ws, update_df1, 'F', 'J', fields_to_updt=['job title', 'supply/solar/tech', 'split', 'sociedad', 'status'], skip_fields=6)
update_fields(ws, update_df2, 'M', 'Q', fields_to_updt=['profile', 'seniority', 'q', 'team', 'sub team'], skip_fields=6)
update_fields(ws, update_df_manager, 'T', 'T', fields_to_updt=['manager'], skip_fields=6)
update_fields(ws, update_df_end_date, 'V', 'V', fields_to_updt=['end date'], skip_fields=6)
update_fields(ws, update_df_salaries, 'Z', 'AA', fields_to_updt=['fix salary','bonus'], skip_fields=6)
