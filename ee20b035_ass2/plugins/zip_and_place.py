import zipfile
import os

def zip_place_delete(zip_dir, **kwargs):
    
    path_to_files = kwargs['ti'].xcom_pull(task_ids = 'fetch_individual_files', key='file_paths')   # Retrieve the paths where the CSVs are saved
    with zipfile.ZipFile(zip_dir, 'w') as zip_file:
        for file in path_to_files:
            zip_file.write(file, arcname=file.split('/')[-1])                                       # Zip the files
    print(f"Zipped the files to {zip_dir}")

    for file_path in path_to_files:                                                                 # Delete the downloaded CSVs since you dont 
        if os.path.isfile(file_path):                                                               # need them anymore after zipping
            os.remove(file_path)
        else:
            print("Error: %s file not found" % file_path)
