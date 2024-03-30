import os

def get_names(unzip_dir):
    filenames = []
    for filename in os.listdir(unzip_dir):          # Gets the names of the csv files
        if filename.endswith('.csv'):
            filenames.append(filename)
    print(filenames)
    return filenames


