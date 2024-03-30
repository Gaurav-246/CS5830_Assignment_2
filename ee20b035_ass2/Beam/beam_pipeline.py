import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from read_the_csv import read_csv_data
from compile_the_data import compile_data
from compute_the_avg import compute_avg
from plot_gifs import plot_fields
from get_csv_names import get_names

required_fields = ["HourlyAltimeterSetting", "HourlyDewPointTemperature", "HourlyDryBulbTemperature"]      # Specify Required Fields
plots_dir = '/home/laog/airflow/plots/'                                                                    # Specify where you want to unzip the CSV files and store the plots (CSV files will be deleted)

def extract_required_data_pipeline(file_names, unzip_dir):
    beam_options = PipelineOptions(
        runner='DirectRunner',
        project='first_beam_project',
        job_name='ocr-multiprocessing',
        temp_location='/tmp',
        direct_num_workers=3,
        direct_running_mode='multi_processing'
        )
                                                                                                    # Gets the correct file names chosen at the start of the DAG
    unzip_dir = plots_dir                                                                           # Gets the path to the unzipped csv files
    csv_paths = []
    for file_name in file_names:    
        csv_paths.append(unzip_dir + file_name)

    #beam_options = PipelineOptions()
    with beam.Pipeline(options=beam_options) as pipeline:
        final_data = []
        for path in csv_paths:                                                                      # Iterate through each csv file
            
            read_and_cleaned_data = (
                pipeline
                | f'read_{path}' >> beam.io.ReadFromText(path, skip_header_lines=1)                 # Reading the csv file line by line as text for efficiency
                | f'clean_{path}' >> beam.Map(read_csv_data, hourly_columns_start=9,hourly_columns_end=9+len(required_fields)-1)     # Choosing the required columns as specified above
                | f'compile_{path}' >> beam.CombineGlobally(compile_data)                           # Compiling it to be processed further(else we cannot compute averages line by line)
            )
            compute_monthly_avg = (
                read_and_cleaned_data
                | f'compute_avg_{path}' >> beam.Map(compute_avg)                                    # Computes the monthly averages in the required format
            )
            final_data.append(compute_monthly_avg)                                                  # Compiles the avrages for all locations (all CSVs)
        plot_field = (                                                                              # (Since each CSV corresponds to 1 location)
        final_data
        | 'Merge results' >> beam.Flatten()
        | 'Add Key' >> beam.Map(lambda x: ('group_all', x)) 
        | 'Group Results by key' >> beam.GroupByKey()
        | 'Plot Heatmap' >> beam.Map(plot_fields, required_fields=required_fields)                  # Plots and converts to GIFs
    )

file_names = get_names(plots_dir) 
extract_required_data_pipeline(file_names=file_names, unzip_dir=plots_dir)