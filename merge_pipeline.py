import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner

import google.auth
from datetime import datetime, timedelta
import json
import numpy
import csv
import logging
logging.getLogger().setLevel(logging.INFO)

# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=['--streaming'])

options.view_as(pipeline_options.StandardOptions).streaming = True
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).region = 'us-west1'
options.view_as(
    GoogleCloudOptions).staging_location = 'gs://samhitha-dataflow/staging'
options.view_as(
    GoogleCloudOptions).temp_location = 'gs://samhitha-dataflow/temp'
options.view_as(GoogleCloudOptions).job_name = 'Vaccination_Progress'


#cleaning

def replace_cols(data):
    """replace with 0"""
    from datetime import date
    data_dict = {}
    if data is None or 'country' in data:
        return None

    data_dict['Country'] = data[0] if data[0] else None
    data_dict['iso_code'] = data[1] if data[1] else 'unknown'

    logging.info("input "+str(data))
    (year, month, day) = data[2].split('-')
    data_dict['Date'] = date(month=int(month), day=int(
        day), year=int(year))
    data_dict['Total_vaccinations'] = float(data[3]) if data[3] != '' else 0
    data_dict['People_vaccinated'] = int(
        float(data[4])) if len(data[4]) > 0 else 0
    data_dict['People_fully_vaccinated'] = int(
        float(data[5])) if data[5] else 0

    data_dict['Daily_vaccinations_raw'] = int(float(data[6])) if data[6] else 0
    data_dict['Daily_vaccinations'] = int(float(data[7])) if data[7] else 0
    data_dict['Total_vaccinations_per_hundred'] = float(
        data[8]) if data[8] else 0

    data_dict['People_vaccinated_per_hundred'] = float(
        data[9]) if data[9] else 0
    data_dict['People_fully_vaccinated_per_hundred'] = float(
        data[10]) if data[10] else 0
    data_dict['Daily_vaccinations_per_million'] = float(
        data[11]) if data[11] else 0

    data_dict['Vaccines'] = data[12] if data[12] else 'unknown'
    data_dict['Source_name'] = data[13] if data[13] else 'unknown'
    data_dict['Source_website'] = data[14] if data[14] else 'unknown'
    #print (data_dict)
    return data_dict


def del_unwanted_cols(data):
    """Deleting unwanted columns"""
    if data is None:
        return None
    del data['Source_name']
    del data['Source_website']
    return data


def format_data_old(data):
    if data is None:
        return None

    data['Date'] = "{:%Y-%m-%d}".format(data['Date'])
    return data


def format_data(data):
    if data is None:
        return None
    d_dict = {}
    d_dict['Country'] = data[0]
    d_dict['Tot_vaccine_max'] = data[1][0][0][0]
    d_dict['Tot_vaccine_max_date'] = "{:%Y-%m-%d}".format(data[1][0][0][1])
    d_dict['People_vaccine_max'] = data[1][1][0][0]
    d_dict['People_vaccine_max_date'] = "{:%Y-%m-%d}".format(data[1][1][0][1])
    d_dict['People_fully_vaccine_max'] = data[1][2][0][0]
    d_dict['People_fully_vaccine_max_date'] = "{:%Y-%m-%d}".format(
        data[1][2][0][1])
    return d_dict


def filter_out_nones(row):
    if row is not None:
        yield row
    else:

        pass


def print_ple_vac(line):
    logging.info("print_ple_vac "+str(line))
    return line

def print_output(line):
    logging.info("Output "+str(line))
    return line


def print_output2(line):
    logging.info("Output2 "+str(line))
    return line


def read_from_file(line):
    import csv
    x = None
    try:

        x = next(csv.reader([line]))
    except Exception as e:
        logging.error(line)
        logging.error(str(e))
    return x


def print_mapa(line):
    logging.error("Map after  "+str(line))
    return line


def print_mapb(line):
    logging.error("Map before  "+str(line))
    return line


#Batch Data Topic
batch_topic = "projects/samhitha-data228-project/topics/vaccines_batch_in"


#Streaming Data Topic
streaming_topic = "projects/samhitha-data228-project/topics/my-dataflow-topic1"
#processing streaming pipeline


with beam.Pipeline(options=options) as pipeline:

    batch_data = (pipeline
                  | "Read Data" >> beam.io.ReadFromPubSub(topic=batch_topic)
                  | "window1" >> beam.WindowInto( beam.window.FixedWindows(600))
                  | "Convert Msg To Dict" >> beam.Map(lambda x: json.loads(x))
                  | "Extract Filename" >> beam.Map(lambda x: 'gs://{}/{}'.format(x['bucket'], x['filename']))
                  | "Read File" >> beam.io.ReadAllFromText()
                  )

    data = pipeline | "read" >> beam.io.ReadFromPubSub(topic=streaming_topic)
    windowed_data = (data | "window" >> beam.WindowInto(
        beam.window.FixedWindows(600)))
    streaming_data = (windowed_data | "decode data" >> beam.Map(lambda x: x.decode("utf-8"))

                      )

    decode_data = (batch_data, streaming_data) | 'flatten' >> beam.Flatten()

    csv_formatted_data = (
        decode_data
        | "Read line" >> beam.Map(read_from_file)
        | "Replace Columns" >> beam.Map(replace_cols)
        | "DelUnwantedData" >> beam.Map(del_unwanted_cols)
        | "Filter Nones" >> beam.ParDo(filter_out_nones)

        #|beam.Map(print)

    )
    # map_and_group_by_country = (
    # csv_formatted_data

    #     | "Forming vacninations date" >> beam.Map(lambda elem: (elem['Country'], ((elem['Total_vaccinations_per_hundred'], elem['Date']), 
    #                                                                               (elem['People_vaccinated_per_hundred'], elem['Date']),
    #                                                                               (elem['People_fully_vaccinated_per_hundred'], elem['Date'])
    #                                                                             )
    #                                                             )
    #                                             )
    #     | "Print mapb" >> beam.Map(print_mapb)
    #     | 'Max Total Vaccination' >> beam.GroupByKey()
        
         
    # )
    # Total_vaccinations_max = (
    #     map_and_group_by_country

    #     # | "Forming vacninations date" >> beam.Map(lambda elem: (elem['Country'], (elem['Total_vaccinations_per_hundred'], elem['Date'])))
    #     # | "Print mapb" >> beam.Map(print_mapb)
    #     # | 'Max Total Vaccination' >> beam.GroupByKey()
    #     # | "Print mapa" >> beam.Map(print_mapa)
    #     | 'Map grouped data' >> beam.Map(lambda elem: (elem[0], max(elem[1][0])))
    #     #|beam.Map(print)
    # )
    # People_vaccinated_max = (
    #     map_and_group_by_country
    #     | "p peop_vac" >>beam.Map(print_ple_vac)
    #     | beam.Map(lambda elem: (elem[0], max(elem[1][1])))
    #     #|beam.Map(print)
    # )
    # People_fully_vaccinated_max = (
    #     map_and_group_by_country
    #     | beam.Map(lambda elem: (elem[0], max(elem[1][2])))
    #     #|beam.Map(print)
    # )



    Total_vaccinations_max = (
        csv_formatted_data

        | "Forming vacninations date" >> beam.Map(lambda elem: (elem['Country'], (elem['Total_vaccinations_per_hundred'], elem['Date'])))
        | "Print mapb" >> beam.Map(print_mapb)
        | 'Max Total Vaccination' >> beam.GroupByKey()
        | "Print mapa" >> beam.Map(print_mapa)
        | 'Map grouped data' >> beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )
    People_vaccinated_max = (
        csv_formatted_data
        | beam.Map(lambda elem: (elem['Country'], (elem['People_vaccinated_per_hundred'], elem['Date'])))
        #|beam.Map(print)
        | 'Max People Vaccinated' >> beam.GroupByKey()
        
        | beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )
    People_fully_vaccinated_max = (
        csv_formatted_data
        | beam.Map(lambda elem: (elem['Country'], (elem['People_fully_vaccinated_per_hundred'], elem['Date'])))
        #|beam.Map(print)
        | 'Max People Fully Vaccinated ' >> beam.GroupByKey()
        
        | beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )

    output2 = (
        Total_vaccinations_max | "Output2 " >> beam.Map(print_output2)

    )
    agg = ((Total_vaccinations_max, People_vaccinated_max, People_fully_vaccinated_max)
        | "Merge Data" >> beam.CoGroupByKey()
        |    "Format Data" >> beam.Map(format_data)
    #|beam.Map(print)
    )
    
    # output = (
    #     csv_formatted_data | "Output " >> beam.Map(print_output)

    # )
    

    # Output to BIG QUERY
    # (format_output
    #   | "Write all data to BigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.Vaccination_Data", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    (agg
     | "Write all data to BigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.vaccinationtrend", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


    DataflowRunner().run_pipeline(pipeline, options=options)
