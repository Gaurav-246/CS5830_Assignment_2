import numpy as np

def compute_avg(data):
    latitude = data[0]
    longitude = data[1]
    num_columns = len(data[3])

    month_data_dict = {1: []}
    for i in range(11):
      month_data_dict[i+2] = []             # Create a dictionary for months and their corresponding averages

    for i in range(2,len(data)):
        if(i%4==2):                         # Every 3rd, 7th, 11th .. value in data is the corresponding value
            month = int(data[i][5:7])
            month_data_dict[month].append(np.array(data[i+1]))

    for i in range(12):
        x = np.nanmean(month_data_dict[i+1], axis=0)   # Compute the means excluding nan values
        month_data_dict[i+1] = x
        if type(x)==np.float64:                        # If entire column is nan, then create nan values for each month
            month_data_dict[i+1] = [np.nan]*num_columns            # These values won't appear on the plot

    return (latitude, longitude, [month_data_dict[i+1] for i in range(12)])