# Making necessary Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import pathlib
import functools as ft
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG( 
 dag_id="Norm_Status_Update", 
 start_date=dt.datetime(2024, 1, 14),
 # below tells Airflow that we want to run this weekly
 schedule_interval='@weekly', 
)

# Connect to the Google sheet directly using Pandas
def connect_to_sheets():
    sheet_ids = ['id1', 'id2',
                'id3']
    sheet_names = ['name1', 'name2', 'name3']

    # You have to allow the sheet to be editable by all from google sheets settings
    urls = [f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}' for sheet_id, sheet_name 
            in zip(sheet_ids, sheet_names)]
    
    df_norm1 = pd.read_csv(urls[0], parse_dates = ['Date'], infer_datetime_format = True)
    df_norm2 = pd.read_csv(urls[1], parse_dates = ['Date'], infer_datetime_format = True)
    df_work = pd.read_csv(urls[2], parse_dates = ['Date'], infer_datetime_format = True)

    dfs = [df_norm1, df_norm2, df_work]
    use_dfs=[]

    for df in dfs:
        # Parsing Dates
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        
        # Removing unnamed columns
        cols=df.columns
        use_cols = [a for a in cols if 'unnamed:' not in a.lower()]
        df = df[use_cols]
        use_dfs.append(df)
    
    # Joining all the dfs on date columns    
    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['Date', 'Day'], how='left'), use_dfs)
    return df_final.drop('Day', axis=1)


def Generate_vizes(**kwargs):
    execution_date = kwargs["execution_date"]    #<datetime> type with timezone
    df=connect_to_sheets()
    # Specify start date as a week from current date
    #today = dt.date.today()
    #start_day = today - dt.timedelta(days=7)
    start_day = execution_date
    today=start_day + dt.timedelta(days=6)
    today = today.strftime("%Y-%m-%d")
    start_day = start_day.strftime("%Y-%m-%d")

    # selecting data from past 7 days till today
    needed_data = df[np.logical_and(df['Date'] >= start_day, df['Date'] <= today)]

    unpack =[needed_data, start_day, today]
    data = unpack[0]

    # Applying function to replace Yes with 1 and No with 0 and also filling Na with 0
    data.fillna(0, inplace=True)
    mapping_dict = {'Yes': 1, 'No': 0}
    df_mapped = data.applymap(lambda x: mapping_dict.get(x, x))

    # Making directory to store saved images
    pathlib.Path("/mnt/c/Users/User/Viz").mkdir(parents=True, exist_ok=True)
    pathlib.Path("/mnt/c/Users/User/Viz/Work").mkdir(parents=True, exist_ok=True)

    # Set the desired columns as the index
    df_mapped.set_index(['Date'], inplace=True)
    df_mapped = df_mapped.iloc[:,1:8]
    df_use=df_mapped.copy().transpose()

    # Creating the Day_count
    df_use['Day_Count']=df_use.sum(axis=1)

    # Creating Visualization
    ax=sns.barplot(x=df_use.index, y=df_use.Day_Count)

    # Printing the number of days at the top of the bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', color='black')
    plt.xlabel('Day_Count')
    plt.xlabel('Norm')
    plt.xticks(rotation=45)
    plt.title('Report for '+unpack[1]+'-'+unpack[2], fontsize=12)
    plt.savefig('/mnt/c/Users/User/Viz/Viz_'+unpack[1]+'-'+unpack[2], dpi=500, bbox_inches= 'tight')



# Creating the sheet connection and visualization into a single task
sheet_connect_viz_gen = PythonOperator( 
 task_id="Generate_Visualizations",
 python_callable=Generate_vizes, 
 dag=dag,
)

# Creating the email function
def email():
    # Still in Progress
    print('Done')

# Creating the email task
send_email = PythonOperator(
    task_id='Notify_via_Email',
    python_callable=email,
    dag=dag
)

# Specifying the order of task execution so Airflow knows which to run first
sheet_connect_viz_gen >> send_email