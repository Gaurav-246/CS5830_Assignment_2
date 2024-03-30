def compile_data(data):
    flattened_list = []
    for sublist in data:                    # Flatten the data
        flattened_list.extend(sublist)
    return flattened_list