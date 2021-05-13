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
options.view_as(GoogleCloudOptions).staging_location = 'gs://samhitha-dataflow/staging'
options.view_as(GoogleCloudOptions).temp_location = 'gs://samhitha-dataflow/temp'
options.view_as(GoogleCloudOptions).job_name = 'Vaccination'








class CollectLocationKey(beam.DoFn):
    def process(self, element):
        if element is None:
            return element
        return [(element['Country'], float(element['Daily_vaccinations']) if element['Daily_vaccinations'] else 0.0 )]






def addKey(row):
    return (1, row)

def sortGroupedData(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x['total_vaccines'], reverse=True)
    sortData_t10 = sortData[0:10]
    sortData_t10 = [dict(d, **{"rank":i+1}) for i,d in enumerate(sortData_t10)]
    return sortData_t10

#FUNCTION FOR TOP VACCINES IN THE WORLD

class CollectLocationKey_Vaccines(beam.DoFn):
    def process(self, element):
        if element is None:
            return element
        return [(element['Country'],element['Vaccines'], float(element['Total_vaccinations']) if element['Total_vaccinations'] else 0.0 )]



def addKey_vaccines(row):
    return (1, row)

def sortGroupedData_vaccines(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x['country_count'], reverse=True)
    sortData_t10 = sortData[0:10]
    sortData_t10 = [dict(d, **{"rank":i+1}) for i,d in enumerate(sortData_t10)]
    return sortData_t10




# DATA CLEANING FUNCTIONS

def replace_cols(data):
    """replace with 0"""
    from datetime import date
    data_dict = {}
    if data is None or 'country' in data :
        return None

    data_dict['Country'] = data[0] if data[0] else None
    data_dict['iso_code'] = data[1] if data[1] else 'unknown'

    #print (data)
    ( year, month,day) = data[2].split('-')
    data_dict['Date'] = date(month=int(month), day=int(
        day), year=int(year))
    data_dict['Total_vaccinations'] = float(data[3]) if data[3] != '' else 0
    data_dict['People_vaccinated'] = int(float(data[4])) if len(data[4]) > 0 else 0
    data_dict['People_fully_vaccinated'] = int(float(data[5])) if data[5] else 0

    data_dict['Daily_vaccinations_raw'] = int(float(data[6])) if data[6] else 0
    data_dict['Daily_vaccinations'] = int(float(data[7])) if data[7] else 0
    data_dict['Total_vaccinations_per_hundred'] = float(data[8]) if data[8] else 0

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
    if data is None :
        return None
    del data['Source_name']
    del data['Source_website']
    return data


def format_data_old(data):
    if data is None:
        return None

    data['Date'] = "{:%Y-%m-%d}".format(data['Date'])
    return data


def format_stat_data(data):
    if data is None:
        return None
    d_dict = {}
    d_dict['Country'] = data[0]
    d_dict['Max_Total_Vaccination'] = data[1][0][0][0]
    d_dict['Total_Vaccination_LastDate'] = "{:%Y-%m-%d}".format(data[1][0][0][1])
    d_dict['Max_People_Vaccinated'] = data[1][1][0][0]
    d_dict['People_Vaccinated_LastDate'] = "{:%Y-%m-%d}".format(data[1][1][0][1])
    d_dict['Max_People_fully_vaccinated'] = data[1][2][0][0]
    d_dict['People_fully_vaccinated_LastDate'] = "{:%Y-%m-%d}".format(
        data[1][2][0][1])
    return d_dict



def filter_out_nones(row):
    if row is not None:
        yield row
    else:

        pass

def print_output(line):
    logging.info("Output "+str(line))
    return line

def print_output2(line):
    logging.info("Output2 "+str(line))
    return line

def print_ple_vac(line):
    logging.info("print_ple_vac "+str(line))
    return line

def print_mapa(line):
    logging.error("Map after  "+str(line))
    return line


def print_mapb(line):
    logging.error("Map before  "+str(line))
    return line

def format_data_old(data):
    if data is None:
        return None

    data['Date'] = "{:%Y-%m-%d}".format(data['Date'])
    return data



def read_from_file(line):
    import csv
    x = None
    try:

        x = next(csv.reader([line]))
    except Exception as e:
        logging.error(line)
        logging.error(str(e))
    return x


def format_csv_data_bq(x):
    data = {
        "country": x['Country'],
        "iso_code": x['iso_code'],
        "date": x['Date'].isoformat(),
        "total_vaccinations": x['Total_vaccinations'],
        "people_vaccinated": x['People_vaccinated'],
        "people_fully_vaccinated":x['People_fully_vaccinated'] ,
        "daily_vaccinations_raw" :x['Daily_vaccinations_raw'],
        "daily_vaccinations": x["Daily_vaccinations"],
        "total_vaccinations_per_hundred" : x['Total_vaccinations_per_hundred'],
        "people_vaccinated_per_hundred":x['People_vaccinated_per_hundred'],
        "people_fully_vaccinated_per_hundred":x['People_fully_vaccinated_per_hundred'],
        "daily_vaccinations_per_million":x['Daily_vaccinations_per_million'],
        "vaccines":x['Vaccines']
        }
    return data



#Batch Data Topic
batch_topic = "projects/samhitha-data228-project/topics/vaccines_batch_in"


#Streaming Data Topic
streaming_topic = "projects/samhitha-data228-project/topics/my-dataflow-topic1"
#processing streaming pipeline


with beam.Pipeline(options=options) as pipeline:

    batch_data = (pipeline
                |"Read_Batch_Data" >> beam.io.ReadFromPubSub(topic=batch_topic)
                | "Window_Batch" >> beam.WindowInto(beam.window.FixedWindows(300))
                |"Convert_To_Dict" >> beam.Map(lambda x: json.loads(x))
                |"Extract_File" >> beam.Map(lambda x : 'gs://{}/{}'.format(x['bucket'], x['filename']))
                |"Read_File" >> beam.io.ReadAllFromText()

                 )

    data = pipeline | "Read_Stream_Data" >> beam.io.ReadFromPubSub(topic=streaming_topic)
    windowed_data = (data | "Window_Stream" >> beam.WindowInto(beam.window.FixedWindows(300)))
    streaming_data = (windowed_data|"Decode_Data" >> beam.Map(lambda x: x.decode("utf-8"))

              )


    

    merge_data = (batch_data, streaming_data) | "Merge_Data" >> beam.Flatten()

    csv_formatted_data = (
                              merge_data
                              | "Read_Line" >> beam.Map(read_from_file)
                              | "Replace_Columns">>beam.Map(replace_cols)
                              | "DelUnwantedData" >> beam.Map(del_unwanted_cols)
                              | "Filter_Nones" >> beam.ParDo(filter_out_nones)
                              #|beam.Map(print)

                            )
    test = (csv_formatted_data
            | 'Count all elements' >> beam.combiners.Count.Globally().without_defaults()
            |  "O1" >>beam.Map(print_output)
            )
            

    events = (csv_formatted_data
          |  "FormatToBigquery" >> beam.Map(format_csv_data_bq)
          | "WriteAllDataToBigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.vaccine_format", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
         )

    
    (csv_formatted_data 
    |  "FormatToBigquery old" >> beam.Map(format_data_old)
    | "Write all data to BigQuery asd" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.Vaccination_Data_new", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    

    grouped_by_country = (csv_formatted_data
                             | "CollectingCountryKey" >> beam.ParDo(CollectLocationKey())
                             | "GroupingByCountry" >> beam.GroupByKey()
                            #|beam.Map(print)
                         )




     # Which country Vaccinating more people?

    Total_vaccinations = (grouped_by_country
                         | "ExtractTotalVaccinations" >> beam.Map(lambda x : {'country':x[0], 'total_vaccines':sum(x[1])})
                  )



    Top_10_country_vaccinations = (Total_vaccinations
                           | 'AddKey' >> beam.Map(addKey)
                           | 'GroupByKey' >> beam.GroupByKey()
                           | 'SortGroupedData' >>  beam.Map(sortGroupedData)
                          # |beam.Map(print)
                          )



    #output = ( Top_10_country_vaccinations
        #| "Output1 " >> beam.Map(print_output)

    #)


    # Most Popular Vaccine in the world?

    Top_Vaccines_data = (csv_formatted_data
                    | "CombineColumns" >> beam.ParDo(CollectLocationKey_Vaccines())
                                                            )


    Top_Vaccines_country = (Top_Vaccines_data
                    | "CollectColumns" >> beam.Map(lambda x: (x[1] , x[0]))
                    | "GroupingByCountry_vaccine" >> beam.GroupByKey()
                    |beam.Map(lambda x: {'Vaccine':x[0] , 'country_count':len(set(x[1]))})

                    |"SortedValues" >>beam.Map(addKey_vaccines)
                    | "GroupByKeyTotalVaccinations" >> beam.GroupByKey()
                    | "SortGroupedDataVaccines" >>  beam.Map(sortGroupedData_vaccines)

                     )



    Total_vaccinations_max = (
        csv_formatted_data

        | "FormingVacninationsDate" >> beam.Map(lambda elem: (elem['Country'], (elem['Total_vaccinations_per_hundred'], elem['Date'])))
        #| "PrintMapb" >> beam.Map(print_mapb)
        | "MaxTotalVaccination" >> beam.GroupByKey()
        #| "Print mapa" >> beam.Map(print_mapa)
        | "MapGroupedData1" >> beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )

    People_vaccinated_max = (
        csv_formatted_data
        | "FormingPeopleVacninationsDate" >>beam.Map(lambda elem: (elem['Country'], (elem['People_vaccinated_per_hundred'], elem['Date'])))
        #|beam.Map(print)
        | "MaxPeopleVaccinated" >> beam.GroupByKey()

        | "MapGroupedData2" >> beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )

    People_fully_vaccinated_max = (
        csv_formatted_data
        | "FormingPeopleFullyVacninationsDate" >> beam.Map(lambda elem: (elem['Country'], (elem['People_fully_vaccinated_per_hundred'], elem['Date'])))
        #|beam.Map(print)
        | "Max PeopleFullyVaccinated" >> beam.GroupByKey()
        | "MapGroupedData3" >> beam.Map(lambda elem: (elem[0], max(elem[1])))
        #|beam.Map(print)
    )

    #output2 = (
       # Total_vaccinations_max | "Output3 " >> beam.Map(print_output2)

    #)


    agg = ((Total_vaccinations_max, People_vaccinated_max, People_fully_vaccinated_max)
           | "CombineData" >> beam.CoGroupByKey()
           | "FormatData" >> beam.Map(format_stat_data)
           #|beam.Map(print)
           )


    



    Top_country_vaccinations = ( Top_10_country_vaccinations
                                | 'FlatCountlists' >> beam.FlatMap(lambda elements: elements)
                                 | "WriteTopCountryBigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.topcountry", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
         )


    Top_vaccine = ( Top_Vaccines_country
                 | 'Flatten lists' >> beam.FlatMap(lambda elements: elements)
                    | "WriteTopVaccine BigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.topvaccine", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
          )



    (agg
     | "Write all data to BigQuery" >> beam.io.WriteToBigQuery("samhitha-data228-project:Vaccine_Dataset.vaccinationtrend", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    
    
    DataflowRunner().run_pipeline(pipeline, options=options)