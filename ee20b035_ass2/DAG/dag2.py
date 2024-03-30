from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from select_random import select_random_files
from fetch_files import fetch_files
from zip_and_place import zip_place_delete
from get_csv_names import get_names

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 2),
    'retries': 0,
}

dag = DAG('data_fetch2',
          default_args=default_args,
          description='A DAG to fetch public domain climatological data from National Centers for Environmental Information for a given year, process it to find monthly averages of weather data and to finally plot them as geospatial heatmaps',
          schedule_interval=None)#'*/1 * * * *')

base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'    
year = '2000'                                                               # Specify which year's data you want to retrieve
required_num_of_files = 5                                                   # Specify number of files to retrieve

page_dir = '/home/laog/airflow/data/'                                       # Specify where you want to download the html page
page_name = 'weather_data_'+year+'.html'                                    # Specify name of the html page when downloaded/saved
data_dir = '/home/laog/airflow/data/'                                       # Specify where you want to download the csv files
zip_dir = '/home/laog/airflow/data/archive.zip'                             # Specify where you want to zip it and name the zip file
plots_dir = '/home/laog/airflow/plots/'                                     # Specify where you want to unzip the CSV files and store the plots (CSV files will be deleted)


task1 = BashOperator(
    task_id = 'download_dataset',
    bash_command = f'curl -L -o {page_dir}{page_name} {base_url}/{year}',    # Page gets saved as weather_data_2000.html
    dag = dag
)

task2 = PythonOperator(
    task_id = 'select_random_files',
    python_callable = select_random_files,                                   # Returns 5 random file names ; Can be seen in logs
    op_kwargs = {'page_path' : r'%s%s'%(page_dir,page_name), 'req_num_of_files' : required_num_of_files},
    provide_context=True,
    dag = dag
)

task3 = PythonOperator(
    task_id = 'fetch_individual_files',
    python_callable = fetch_files,                                           # Downloads the 5 csv files to the specified directory
    op_kwargs = {'base_link' : base_url, 'data_dir' : data_dir, 'year' : year},
    provide_context = True,
    dag = dag
)

task4 = PythonOperator(                      
    task_id = 'zip_files',
    python_callable = zip_place_delete,                                          # Zips the 5 csv files to the specified directory
    op_kwargs={'zip_dir' : zip_dir},
    provide_context = True,
    dag = dag
)

task5 = FileSensor(
    task_id = 'check_for_archive_availability',                                  # Checks if the zip file exists
    filepath = zip_dir,                                            
    poke_interval = 1,  
    timeout = 5,         
    mode='poke',
)

task6 = BashOperator(
    task_id = 'unzip_files',
    bash_command = f'unzip {zip_dir} -d {plots_dir}',                                            # Unzips the files to a plots directory                                                      
    dag = dag                                                                                    
)

task7 = BeamRunPythonPipelineOperator(
    task_id = 'beam_pipeline',  
    py_file = '/home/laog/airflow/plugins/__pycache__/beam_pipeline.py',        # Beam pipeline to process the data and plot
    py_requirements=['apache-beam', 'geopandas', 'matplotlib'],
    dag=dag
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7