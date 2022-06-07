import configparser
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
import os
import glob
import boto3
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('decp.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

aws_key_id=os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key=os.environ['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Description: 
        Create spark session with hadoop-aws package and return spark.

    Returns:
        spark    
    """
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
        .config("spark.hadoop.fs.s3a.fast.upload","true") \
        .getOrCreate()
    return spark

# def read_i94_descriptions(i94_file):
#     with open(i94_file) as f:
#         f_content = f.read()
#         f_content = f_content.replace('\t', '')
#     return f_content
    

def code_mapper(f_content, idx):
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def process_immigration_events(spark, input_data, output_data):
    """
    Description: 
        Load immigration envents data from S3, process the data into analytic tables using Spark, and load them back into S3.

    Arguments:
        spark: spark session.
        input_data: the root path of source data from S3.
        output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
#     # get the path of the immigration files on local machine
#     fpath="./data/18-83510-I94-Data-2016/"
#     immigration_files=glob.glob(fpath +'*.sas7bdat')
#     print(immigration_files)
#     # read the immigration files and save date to parquet on local machine
#     for i,f in enumerate(immigration_files):
#         if i > 0:
#             print(f)
#     #       df_immigration = pd.read_sas(f, 'sas7bdat', encoding='ISO-8859-1', chunksize=20)
#             df_immigration = pd.read_sas(f, 'sas7bdat', encoding='ISO-8859-1')
#             df_immigration.to_parquet('./immig_parq_temp/df_immigration_' + f[-18:] + '.parquet')
#         else:
#             continue
#     print("-------------")        
    df_immigration = spark.read.parquet('./immig_parq_temp')
#     df_immigration = df_immigration.sample(fraction=0.000005)
    print("------------------++++++++++++++")    
    df_immigration = df_immigration.withColumn("cicid", df_immigration["cicid"].astype('bigint'))
    df_immigration = df_immigration.withColumn("i94yr", df_immigration["i94yr"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94mon", df_immigration["i94mon"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94cit", df_immigration["i94cit"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94res", df_immigration["i94res"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("arrdate", df_immigration["arrdate"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94mode", df_immigration["i94mode"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("depdate", df_immigration["depdate"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94bir", df_immigration["i94bir"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("i94visa", df_immigration["i94visa"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("count", df_immigration["count"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("dtadfile", df_immigration["dtadfile"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("biryear", df_immigration["biryear"].cast(IntegerType()))
    df_immigration = df_immigration.withColumn("admnum", df_immigration["admnum"].astype('bigint'))
    df_immigration = df_immigration.withColumn("depdate", df_immigration["depdate"].astype('int'))
    
    # transform the date data and extract columns to create table
    df_immigration = df_immigration.selectExpr("*", "date_add('1960-01-01',arrdate) as arrival_date",\
                         "date_add('1960-01-01',depdate) as departure_date").drop('arrdate','depdate')
    
    df_immigration = df_immigration.sort('i94yr','i94mon','cicid')
    
    # the "cicid" column be set as the primary key and be auto-incrementing
    df_immigration = df_immigration.withColumn("cicid", monotonically_increasing_id())
        
    # extract columns to create table
    immigration_table = df_immigration.select('cicid', 'i94yr',   'i94mon',  'i94cit',  'i94res',  'i94port',  'arrival_date',  
                                            'departure_date', 'i94mode',  'i94addr',   'i94visa','matflag', 'airline',  'admnum',  'fltno')
    immigrant_table = df_immigration.select('admnum', 'i94bir', 'gender', 'biryear', 'visatype').dropDuplicates()
    
    # To filter out immigrant_table who has been recorded by immigration for many times,
    # but the information recorded before and after is inconsistent
    admnum_error = immigrant_table.groupby('admnum').count().orderBy(col("count").desc()).where('count > 1')
    pd_admnum_error = admnum_error.toPandas()
    rows = pd_admnum_error['admnum'].tolist()
    
    # To filter out the immigration_table what has the error Admission Number
    immigration_table =  immigration_table.filter(~col('admnum').isin(rows))
    
    # To filter out the immigrant_table what has the error Admission Number
    immigrant_table = immigrant_table.filter(~col('admnum').isin(rows))
    
    immigration_table = immigration_table.fillna(0)
    immigrant_table = immigrant_table.fillna(0)
    
    # write table to parquet files 
    immigration_table.repartition(4).write.parquet(output_data +'immigration.parq', mode="overwrite")
    immigrant_table.repartition(1).write.parquet(output_data +'immigrant.parq', mode="overwrite")
    

def process_i94_descriptions(bucket_name, s3_output_data):
    """
    Description: 
        Load airport data from S3, process the data into analytic tables using Pandas, and load them back into S3.

    Arguments:
        bucket_name: the root path of source data from S3.
        s3_output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath
    city_files = os.path.join(bucket_name,'I94_SAS_Labels_Descriptions.SAS')
    with open(city_files) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')   
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business',
                '2': 'Pleasure',
                '3' : 'Student'}
    
    df_i94cit_res= pd.DataFrame(list(i94cit_res.items()),columns = ['i94cit_res','origin_travelled_country'])
    df_i94cit_res.to_csv(s3_output_data + 'df_i94cit_res.csv',index=False)
    
    df_i94port= pd.DataFrame(list(i94port.items()),columns = ['i94port','destination_city'])
    df_i94port['city'] = df_i94port['destination_city'].str.split(',', expand=True)[0]
    df_i94port['region'] = df_i94port['destination_city'].str.split(',', expand=True)[1]
    df_i94port.drop(['destination_city'], axis=1, inplace=True)
    df_i94port.to_csv(s3_output_data + 'df_i94port.csv',index=False)
    
    df_i94mode= pd.DataFrame(list(i94mode.items()),columns = ['i94mode','transportation'])
    df_i94mode.to_csv(s3_output_data + 'df_i94mode.csv',index=False)
    
    df_i94addr= pd.DataFrame(list(i94addr.items()),columns = ['i94addr','state'])
    df_i94addr.to_csv(s3_output_data + 'df_i94addr.csv',index=False)
    
    df_i94visa= pd.DataFrame(list(i94visa.items()),columns = ['i94visa','immigration_reason'])
    df_i94visa.to_csv(s3_output_data + 'df_i94visa.csv',index=False)
    
    

def process_airport_data(bucket_name, s3_output_data):
    """
    Description: 
        Load airport data from S3, process the data into analytic tables using Pandas, and load them back into S3.

    Arguments:
        bucket_name: the root path of source data from S3.
        s3_output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath to airport data file
    airport_files = os.path.join(bucket_name, 'airport-codes_csv.csv')
    
    # read airport data file
    df_airport = pd.read_csv(airport_files, sep=',')
    
    # To filter the closed airports
    airport_table = df_airport[~df_airport['type'].isin(['closed'])]

    # To filter the airports in US
    airport_table = airport_table[airport_table['iso_country']=='US']
    
    airport_table = airport_table[~airport_table['local_code'].isna()]
    
    airport_table['longitude'] = airport_table['coordinates'].str.split(',', expand=True)[0].astype('float').round(2)
    airport_table['latitude'] = airport_table['coordinates'].str.split(',', expand=True)[1].astype('float').round(2)
    airport_table['iso_region'] = airport_table['iso_region'].str.split('-', expand=True)[1]
    
    airport_table.drop(['coordinates','continent','iata_code'], axis=1, inplace=True)
    
    # drop duplicates of local_code, and keep the first
    airport_table = airport_table.drop_duplicates(subset=['local_code'], keep='first', inplace=False)
    
    airport_table = airport_table.reset_index()
    airport_table.rename(columns = {'index':'airport_id'}, inplace = True)
    
    airport_transformed = os.path.join(s3_output_data, 'us_airport.csv')
#     print(airport_transformed)
    airport_table.to_csv(airport_transformed, index=False)
#     print('airport_table has been saved')


def process_temperature_data(bucket_name, s3_output_data):
    """
    Description: 
        Load temperature data from S3, process the data into analytic tables using Pandas, and load them back into S3.

    Arguments:
        bucket_name: the root path of source data from S3.
        s3_output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath to temperature data file
    temperature_files = os.path.join(bucket_name, 'GlobalLandTemperaturesByCity.csv')

    
    # read temperature data file
    df_temperature = pd.read_csv(temperature_files, sep=',')
    
    # To filter out the rows what have null value
    temperature_table = df_temperature[~df_temperature['AverageTemperature'].isna()]
    
    # transport the type of 'dt' to <datetime>
    temperature_table.loc[:,'dt'] = pd.to_datetime(temperature_table.loc[:,'dt'])
    
    # In view of the impact of human activities on climate in recent years, we screened the data after 1985 
    # to calculate the average temperature
    temperature_table = temperature_table[(temperature_table['dt']>'1985-12-31')]
    
    temperature_table['month'] = temperature_table['dt'].dt.month
    
    # delete the letter E what in the columns of 'latitude' and 'longitude'
    temperature_table.loc[:,'Latitude'] = temperature_table.apply(lambda x: x['Latitude'][:-1], axis = 1)
    temperature_table.loc[:,'Longitude'] = temperature_table.apply(lambda x: x['Longitude'][:-1], axis = 1)
    
    temperature_table['Longitude'] = '-' + temperature_table['Longitude'] 
    
    temperature_table['Latitude'] = temperature_table['Latitude'].astype('float')
    temperature_table['Longitude'] = temperature_table['Longitude'].astype('float')
    
    # calculate the mean of city's temperature of each month
    temperature_table_month=temperature_table.groupby(['Country','City','month','Latitude','Longitude'],as_index=False)['AverageTemperature'].mean().round(3)
    
    temperature_table_month = temperature_table_month.reset_index()
    temperature_table_month.rename(columns = {'index':'temperature_id'}, inplace = True)
    
    # write temperature of month table to S3
    temperature_table_month.to_csv(s3_output_data + 'temperature_table_month.csv',index=False)
    

def process_demographic_data(bucket_name, s3_output_data):
    """
    Description: 
        Load demographic data from S3, process the data into analytic tables using Pandas, and load them back into S3.

    Arguments:
        bucket_name: the root path of source data from S3.
        s3_output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath to demographic data file
    demographic_files = os.path.join(bucket_name, 'us-cities-demographics.csv')
   
    # read demographic data file
    df_demographic = pd.read_csv(demographic_files,sep=';')
    
    # To filter out the columns of 'State Code, Race, Count', and merge the duplicates
    demographic_table = df_demographic.iloc[:,0:9].drop_duplicates()
    demographic_table = demographic_table.fillna(0)
    demographic_table['Male Population'] = demographic_table['Male Population'].astype('int')
    demographic_table['Female Population'] = demographic_table['Female Population'].astype('int')
    demographic_table['Number of Veterans'] = demographic_table['Number of Veterans'].astype('int')
    demographic_table['Foreign-born'] = demographic_table['Foreign-born'].astype('int')
    
    demographic_table = demographic_table.reset_index()
    demographic_table.rename(columns = {'index':'demographic_id'}, inplace = True)
    
    # write demographic table to S3
    demographic_table.to_csv(s3_output_data + 'demographic_table.csv',index=False)
    
def load_staging_tables(cur, conn):
    """
    Description: 
        Load data on AWS to staging tables using the queries in 'copy_table_queries' list.

    Arguments:
        cur: the cursor object. 
        conn: the connection of the database.
        
    Returns:
        None
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: 
        Insert data of staging tables to the analytics tables using the queries in 'copy_table_queries' list.

    Arguments:
        cur: the cursor object. 
        conn: the connection of the database.

    Returns:
        None
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    
def main():
    """
    The main function callbacks create_spark_session() and creates the path of input_data and output_data where they are on S3, 
    then call function of process_song_data() and process_log_data() to use them.
    """
    spark = create_spark_session()
    input_data = "s3a://lcw-udacity-capstone-project/"
#     output_data == 's3a://lcw-udacity-capstone-project/transformed/'
    output_data = './transformed/'
    
#     client = boto3.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key)

    bucket_name = './source/'
    s3_output_data = './transformed/'
#     s3_output_data = 's3://lcw-udacity-capstone-project/transformed/'
    process_immigration_events(spark, input_data, output_data)    
    process_i94_descriptions(bucket_name, s3_output_data)
    process_airport_data(bucket_name, s3_output_data)
    process_temperature_data(bucket_name, s3_output_data)
    process_demographic_data(bucket_name, s3_output_data)


#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
    
#     load_staging_tables(cur, conn)
#     insert_tables(cur, conn)

#     conn.close()


if __name__ == "__main__":
    main()
