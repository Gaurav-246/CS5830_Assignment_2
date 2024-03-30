import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from matplotlib.animation import FuncAnimation

def plot_fields(data, required_fields):  
    key = data[0]                                   # This is the key used by GroupByKey()
    num_of_coords = len(data[1])
    num_of_fields = len(required_fields)
    lat_list=[]
    long_list=[]
    field_list = []
    value_list = []
    for k in range(num_of_coords):                  # Creating a dataframe of the values
        lat, long, arr = data[1][k]
        arr = np.array(arr)
        arr = arr.T
        for i in range(num_of_fields):
            for j in range(12):
                lat_list.append(lat)                # Create a list of latitudes to later convert into a dictionary
                long_list.append(long)              # Create a list of longitudes to later convert into a dictionary
                field_list.append(required_fields[i])       # Create a list of the corresponding field 
                value_list.append(arr[i][j])                # Create a list of the corresponding value for the field
    df = {'Latitude':lat_list, 'Longitude':long_list, 'Values':value_list,'Fields':field_list}          # Convert to a dictionary to directly create a GeoDataFrame
    geometry = gpd.points_from_xy(df['Longitude'], df['Latitude'])
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))   # World map to plot

    for field in required_fields:
        field_gdf = gdf[gdf['Fields'] == field]
        
        overall_vmin = field_gdf['Values'].min()                            # Find the min and max value so that each colorbar remains same throughout the 12 frames of the GIF
        overall_vmax = field_gdf['Values'].max()    
        location_changes = field_gdf[['Latitude', 'Longitude']].diff().ne(0).any(axis=1)     # To identify the locations and values to plot
        location_indices = location_changes[location_changes].index.tolist()                 # Their corresponding indices
        for id in range(len(location_indices)):
            location_indices[id] -= 1
        
        def init_func():
            pass
        def update(frame):                                                  # Update function to make into a GIF
            ax.clear()
            idx = frame
            
            for id in range(len(location_indices)):
                location_indices[id] += 1
            new_gdf = field_gdf.loc[location_indices]
            world.plot(ax=ax, color='lightgrey',edgecolor='#3b3b3b')        # plot world map
            plot = new_gdf.plot(ax=ax, column='Values', cmap='plasma', legend=False, markersize=150, vmin=overall_vmin, vmax=overall_vmax, edgecolor='black')    # Plot values       
            ax.set_title('Month ' + str(idx+1) + ' for '+field, fontsize=30)

        fig, ax = plt.subplots(figsize=(30,14))
        plt.subplots_adjust(left=0.1, right=0.95, top=0.90, bottom=0.05)   # Adjustments for plot size (ignore)
        norm = Normalize(vmin=overall_vmin, vmax=overall_vmax)             # Colorbar settings to make sure only 1 colorbar appears
        sm = ScalarMappable(norm=norm, cmap='plasma')
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, shrink=0.8)
        cbar.ax.tick_params(labelsize=16) 
        animation = FuncAnimation(fig, update, init_func=init_func, frames=12, interval=1000)    # Using FuncAnimation to make the GIF
        animation.save(field+'.gif', writer='pillow')
        print('GIF for field "'+field+'" created!')