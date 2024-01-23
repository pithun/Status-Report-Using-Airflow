import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# We first of all create a DAG object and from what I see, it seems Airflow has different kinds of operators
# and they should all reference the DAG object
dag = DAG( 
 dag_id="download_rocket_launches", 
 start_date=airflow.utils.dates.days_ago(14), 
 schedule_interval=None, 
)

# We use the bash operator to instantiate a bash command to get the json file containing the launches
download_launches = BashOperator( 
 task_id="download_launches", 
 bash_command="curl -o /tmp/launches.json -L \'https://ll.thespacedevs.com/2.0.0/launch/upcoming\'",
 dag=dag,
)

# We define a python function that dowloads the picture and later, we pass a function into the python
# Operator. It seems that's what it was used for.
def _get_pictures(): 
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator( 
 task_id="get_pictures",
 python_callable=_get_pictures, 
 dag=dag,
)

notify = BashOperator(
 task_id="notify",
 bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
 dag=dag,
)

download_launches >> get_pictures >> notify 