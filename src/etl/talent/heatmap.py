import numpy as np
import pandas as pd
import seaborn as sns
import seaborn as sb
import matplotlib as mpl
import matplotlib.pyplot as plt
from holaluz_datatools.sql import PostgreSQLClient
from holaluz_datatools.credentials import load_credentials
import pip
pip.main(["install","matplotlib"])
import pip
pip.main(["install","seaborn"])

df = pd.read_excel("C:/Users/angela.belencoso/Desktop/cal_3.xlsx")

conditions = [(df['Performance'] == 3) & (df['Potential'] == 3),
(df['Performance'] == 3) & (df['Potential'] == 2),
(df['Performance'] == 3) & (df['Potential'] == 1),
(df['Performance'] == 2) & (df['Potential'] == 3),
(df['Performance'] == 2) & (df['Potential'] == 2),
(df['Performance'] == 2) & (df['Potential'] == 1),
(df['Performance'] == 1) & (df['Potential'] == 1)]

choices  = [ "rock_star", 'singer_star', 'vocal_coach','future_rock_star','choir_singer','solist_solid','out_of_tune' ]    
df["seven_box"] = np.select(conditions, choices, default=np.nan)

performance2name_mapping = {3: "High Performance", 2: "Achieve Performance", 1: "Low Performance"}
df['Performance'] = df['Performance'].map(performance2name_mapping)

potential2name_mapping = {3: "High Potential", 2: "Potential", 1: "At Potential"}
df['Potential'] = df['Potential'].map(potential2name_mapping)



conditions = [(df['High Performance'] == 3) & (df['High Potential'] == 3),
(df['High Performance'] == 3) & (df['Potential'] == 2),
(df['High Performance'] == 3) & (df['At Potential'] == 1),
(df['Achieve Performance'] == 2) & (df['High Potential'] == 3),
(df['Achieve Performance'] == 2) & (df['Potential'] == 2),
(df['Achieve Performance'] == 3) & (df['At Potential'] == 3),
(df['Low Performance'] == 1) & (df['At Potential'] == 1)]

choices  = [ "rock_star", 'singer_star', 'vocal_coach','future_rock_star','choir_singer','solist_solid','out_of_tune' ]    
df["seven_box"] = np.select(conditions, choices, default=np.nan)

Performance = ["High","Achieve","Low"]
Potential = ["At Potential","Potential"," High Potential"]

seven_box = np.array([[1,2,3],
                      [1,2,3],
                      [1,np.nan,np.nan]])

labels = seven_box/np.nansum(seven_box)

ax= sns.heatmap(seven_box, fmt=".2%", annot=labels, cmap="YlGnBu", linewidths=1, linecolor='black', xticklabels=Potential, yticklabels=Performance).set(title='7 Box Talent Matrix')
plt.show()


sns.heatmap(df, linewidths=4, linecolor='green')


# Show all ticks and label them with the respective list entries
ax.set_xticks(np.arange(len(Performance)), labels=Performance)
ax.set_yticks(np.arange(len(Potential)), labels=Potential)


# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(), rotation=90, ha="right",
         rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(Performance)):
    for j in range(len(Potential)):
        text = ax.text(j, i, seven_box[i, j],
                       ha="center", va="center", color="w")

ax.set_title("Talent Matrix")
fig.tight_layout()
plt.show()

credentials = load_credentials(credentials_fp = 'C:/Users/Administrator/creds/creds_people.yml')
postgresql_client = PostgreSQLClient(**credentials['people_write'], lazy_initialization = True)
postgresql_client.write_table(
df, 
    "TAL_TALENTMATRIX_FT_New", 
    "people", 
    if_exists = 'replace' # see the different values that if_exists can take in the method docsting
)