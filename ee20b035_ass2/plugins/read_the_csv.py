import numpy as np

def read_csv_data(line, hourly_columns_start, hourly_columns_end):
    
    words = line.split(',')                         # Splits the initial line by commas
    words = [word.strip('"') for word in words]     # Removes the double quotes since all values are stored as strings
    date = words[1]
    words = [float(item) if item.replace('.', '', 1).lstrip('+-').isdigit() else np.nan for item in words]      # Convert strings to numbers
    lat = words[2]
    longitude = words[3]
    select_words = words[hourly_columns_start : hourly_columns_end+1]       # Select the values of the needed columns
    
    return (lat, longitude, date, select_words)