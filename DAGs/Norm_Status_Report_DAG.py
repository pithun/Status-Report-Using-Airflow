import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import pathlib
from airflow import DAG
from airflow.operators.python import PythonOperator

'''This DAG is scheduled to be run every Sunday by 2pm'''

dag = DAG( 
 dag_id="Norm_Status_Update", 
 start_date=dt.datetime(2024, 1, 14),
 # below tells Airflow that we want to run this weekly
 schedule_interval='@weekly', 
)

# Connect to the Google sheet directly using Pandas
# We can actually use arguments in the python function used in airflow DAG.
def connect_to_sheet():
    sheet_id = 'input_your_sheetid'
    sheet_name = 'Norms'
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    df = pd.read_csv(url, parse_dates = ['Date'], infer_datetime_format = True) 
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    return df

# Create Visualizations and report on a weekly basis from startdate of sheet
def Generate_Report():
    df=connect_to_sheet()
    # gets the visualizations from a week from today
    today = dt.date.today()
    start_day = today - dt.timedelta(days=7)
    today = today.strftime("%Y-%m-%d")
    start_day = start_day.strftime("%Y-%m-%d")

    # selecting data from past 7 days till today
    needed_data = df[np.logical_and(df['Date'] >= start_day, df['Date'] <= today)]
    return needed_data, start_day, today

def Generate_vizes():
    unpack =Generate_Report()
    data = unpack[0]

    # Applying function to replace Yes with 1 and No with 0 and also filling Na with 0
    data.fillna(0, inplace=True)
    mapping_dict = {'Yes': 1, 'No': 0}
    df_mapped = data.applymap(lambda x: mapping_dict.get(x, x))

    # Making directory to store saved images
    pathlib.Path("/mnt/c/Users/User/Viz").mkdir(parents=True, exist_ok=True)
    # 1. I have to transform the data such that I can get the day count of each norm
    # Set the desired columns as the index
    df_mapped.set_index(['Date'], inplace=True)
    df_mapped = df_mapped.iloc[:,1:6]
    df_use=df_mapped.copy().transpose()
    #display(df_use)

    # Creating the Day_count
    df_use['Day_Count']=df_use.sum(axis=1)

    # Creating Visualization
    ax=sns.barplot(x=df_use.index, y=df_use.Day_Count)
    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', color='black')
    plt.xlabel('Day_Count')
    plt.xlabel('Norm')
    plt.xticks(rotation=45)
    plt.title('Report for '+unpack[1]+'-'+unpack[2], fontsize=12)
    plt.savefig('/mnt/c/Users/User/Viz/Viz_'+unpack[1]+'-'+unpack[2], dpi=500, bbox_inches= 'tight')
    # 2. I also need a visual of the day and norm/work count I'll need the data in it's original form

    
sheet_connect_viz_gen = PythonOperator( 
 task_id="Generate_Visualizations",
 python_callable=Generate_vizes, 
 dag=dag,
)

def email():
    print('Done')

send_email = PythonOperator(
    task_id='Notify_via_Email',
    python_callable=email,
    dag=dag
)

sheet_connect_viz_gen >> send_email