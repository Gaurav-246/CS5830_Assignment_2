import requests

def fetch_files(base_link, data_dir, year, **kwargs):
    
    ti = kwargs['ti']
    file_paths = []
    file_names = ti.xcom_pull(task_ids = 'select_random_files', key='random_file_names')
    print("Files pulled from XCom are : ",file_names)                       # Pull file names from XCom
    for file_name in file_names:    
        file_link = base_link + year + '/' + file_name
        file_path = data_dir + file_name
        try : 
            r = requests.get(file_link)                                         # Download the files
            r.raise_for_status()                                                # In case of HTTP errors
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(1024):                              # Using chunks to avoid memory issues (in case of big files)
                    f.write(chunk)                                              # Save it in the required directory
            print(f"CSV file successfully downloaded to : {file_path}")
            file_paths.append(file_path)
        except requests.exceptions.HTTPError as e:
            if r.status_code == 404:
                print(f"File not found: {file_link}")
            else:
                print(f"Error in downloading file from {file_link}: {e}")
            print("Moving on to download next file")
            continue
    ti.xcom_push(key='file_paths', value=file_paths)                        # Pushes the downloaded file paths to xcom
    print("File paths to CSVs on local machine pushed are : ",file_paths)   # This will be used to locate the files to zip in the next task