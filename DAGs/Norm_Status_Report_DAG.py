# Making necessary Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import pathlib
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG( 
 dag_id="Norm_Status_Update", 
 start_date=dt.datetime(2024, 1, 14),
 # below tells Airflow that we want to run this weekly
 schedule_interval='@weekly', 
)

# Connect to the Google sheet directly using Pandas
def connect_to_sheet():
    sheet_id = 'input_your_sheetid'
    sheet_name = 'Norms'
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    df = pd.read_csv(url, parse_dates = ['Date'], infer_datetime_format = True) 
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    return df

# Implementing required functions
def Generate_Report():
    df=connect_to_sheet()
    # Specify start date as a week from current date
    #today = dt.date.today()
    #start_day = today - dt.timedelta(days=7)
    start_day = {{execution_date}}
    today=start_day + dt.timedelta(days=7)
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

    # Set the desired columns as the index
    df_mapped.set_index(['Date'], inplace=True)
    df_mapped = df_mapped.iloc[:,1:6]
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