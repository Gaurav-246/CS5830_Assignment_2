from bs4 import BeautifulSoup
import random

def select_random_files(page_path, req_num_of_files, **kwargs):
    
    with open(page_path, 'r') as f:
        page_content = f.read()
    
    soup = BeautifulSoup(page_content, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a')]               # Links in HTML are <a> tags
    links = [link for link in links if link and link.endswith('.csv')]      # Selecting only links with .csv extension
    random_files = random.sample(links, req_num_of_files)                   # Selecting random file names from all the retrieved files
    print("Random files selected are : ", random_files)                     # Log the data
    kwargs['ti'].xcom_push(key='random_file_names', value = random_files)   # Push it to Xcom to be retrieved in the next task to download them
    print("Files pushed to XCom are : ", random_files)                      # Verify pushing into Xcom