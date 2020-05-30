from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField as sf, DateType as da, IntegerType as i, StringType as st, DoubleType as dec, LongType as lt, TimestampType as tt
from pyspark.sql.functions import udf, to_date, col
import datetime
import configparser
import os
import pandas as pd
import re
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError

def create_spark_session():
    '''Initiates a spark session'''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark

def airport_codes_processing(spark,input_data,output_data):
    '''This module loads airport-codes from a csv file and then writes back the parquet files to S3'''
    
    df_airport = spark.read\
                         .format("csv")\
                         .option("header", "true")\
                         .load(input_data)
    col1 = ['ident','type','name','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','iata_code','coordinates']
    col2 = ['ident','type','name','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','local_code','coordinates']
    df_1_new = df_airport.filter("iso_country == 'US'").select(col1).where(df_airport.iata_code.isNotNull())
    df_1_new = df_1_new.withColumnRenamed('iata_code','port_code')
    df_2_new = df_airport.filter("iso_country == 'US'").select(col2).where(df_airport.local_code.isNotNull())
    df_2_new = df_2_new.withColumnRenamed('local_code','port_code')
    df_airport = df_1_new.union(df_2_new)
    df_airport = df_airport.filter(df_airport.port_code.isNotNull()).drop_duplicates(subset=['port_code'])
    df_airport = df_airport.withColumn('state', split('iso_region',"-")[1]).drop('iso_region')
    
    df_airport.write.mode("overwrite").partitionBy("state").parquet(output_data + "airports/airports.parquet")
    
def city_demographics_processing(spark,input_data,output_data):
    '''This module loads us city demographics from a csv file and then writes back the parquet files to S3'''
    
    demographics_schema = StructType ([
        sf("City",st()),
        sf("State", st()),
        sf("median_age", dec()),
        sf("male_population", i()),
        sf("female_population", i()),
        sf("total_population", i()),
        sf("number_of_veterans", i()),
        sf("Foreign_born", i()),
        sf("Average_Household_Size", dec()),
        sf("State_Code", st()),
        sf("Race", st()),
        sf("Count", i())
    ])
    df_demographics = spark.read.csv(input_data, header='true', sep=";", schema=demographics_schema)
    df_demographics = df_demographics.filter(df_demographics.State.isNotNull()).drop_duplicates(subset=['City','State','Race', 'State_Code'])
    
    df_demographics.write.mode("overwrite").partitionBy("State", "City").parquet(output_data + "demographics/demographics.parquet")

def country_table_processing(spark,input_data, output_data):
    '''This module processes country codes from the data dictionary file and writes them to parquet files in S3'''
                                                                                 
    df_country = pd.read_csv(input_data, sep=" =  ", header=None, engine='python',  names = ["country_code", "country"], skipinitialspace = True)
    df_country["country"] = df_country["country"].replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*"], value="NA", regex=True)
    df_country["country"] = df_country["country"].str.replace("'","")
    df_country.drop_duplicates(subset=['country_code'], inplace=True)
    df_country = spark.createDataFrame(df_country)                                                                            
    
    df_country = df_country.filter(df_country.country_code.isNotNull()).drop_duplicates(subset=['country_code'])
    df_country.write.mode("overwrite").partitionBy("country").parquet(output_data + "country/country.parquet")
    
def port_of_entry_processing(spark, input_data, output_data):
    '''This module processes port of entry codes from the data dictionary file and writes them to parquet files in S3'''                                                                      
    df_port = pd.read_csv(input_data, sep="	=	", header=None, engine='python',  names = ["port_of_entry", "place"], skipinitialspace = True)
    df_port["place"] = df_port["place"].replace(to_replace=["Collapsed.*", "No PORT Code.*"], value="NA", regex=True)
    df_port["place"] = df_port["place"].str.replace("'","")
    df_port["port_of_entry"] = df_port["port_of_entry"].str.replace("'","")
    df_port['city'] = df_port['place'].str.split(",",expand=True)[0]
    df_port['state_code'] = df_port['place'].str.split(",",expand=True)[1]
    df_port = df_port.drop(['place'],axis=1)
    df_port = spark.createDataFrame(df_port)

    df_port = df_port.filter(df_port.port_of_entry.isNotNull()).drop_duplicates(subset=['port_of_entry'])
    df_port.write.mode("overwrite").partitionBy("city").parquet(output_data + "portOfEntry/portOfEntry.parquet")
                                                                
def state_table_processing(spark, input_data, output_data):                                                               
    '''This module processes state codes from the data dictionary file and writes them to parquet files in S3'''                     
    df_state = pd.read_csv(input_data, sep="=", header=None, engine='python',  names = ["state_code", "state"], skipinitialspace = True)
    df_state['state_code'] = df_state['state_code'].str.strip('\t')
    df_state["state_code"] = df_state["state_code"].str.replace("'","")
    df_state["state"] = df_state["state"].str.replace("'","")
    df_state = spark.createDataFrame(df_state)
    
    df_state = df_state.filter(df_state.state_code.isNotNull()).drop_duplicates(subset=['state_code'])
    df_state.write.mode("overwrite").partitionBy("state").parquet(output_data + "state/state.parquet")      

def immigration_table_processing(spark, input_data, output_data):                                                               
    '''This module loads immigration data from a sas file, merges the transport mode codes and visa codes from data dictionary and then writes back the parquet files to S3. '''
                                                                  
    #df_immigration = spark.read.csv(input_data, header='true').drop('_c0')
    df_immigration = spark.read.format("com.github.saurfang.sas.spark").load(input_data)
    calDate = udf(lambda t: datetime.datetime.fromtimestamp(int(float(t))).strftime("%x"))
    dateType =  udf (lambda x: datetime.datetime.strptime(x, '%m/%d/%y'), da())
    df_immigration = df_immigration.withColumn('arrdate',calDate(df_immigration.arrdate))
    df_immigration = df_immigration.withColumn('arrdate',dateType(df_immigration.arrdate))
    df_immigration = df_immigration.filter(df_immigration.depdate.isNotNull()).withColumn('depdate',calDate(df_immigration.depdate))
    df_immigration = df_immigration.withColumn('depdate',dateType(df_immigration.depdate))
    remNonAlphaChars = udf(lambda s:re.sub(r'\W+', '', s))
    df_immigration = df_immigration.filter(df_immigration.dtaddto.isNotNull()).withColumn('dtaddto',remNonAlphaChars(df_immigration.dtaddto))
    splitDate = udf(lambda s: s[:2] + "/" + s[2:4] + "/" + s[4:] if s.lower().islower() == False and len(s) == 8 else "INVALID")
    date4Type =  udf (lambda x: datetime.datetime.strptime(x, '%m/%d/%Y'), da())
    df_immigration = df_immigration.withColumn('dtaddto',splitDate(df_immigration.dtaddto))
    df_immigration = df_immigration.where(df_immigration.dtaddto != "INVALID")
    df_immigration = df_immigration.withColumn('dtaddto',date4Type(df_immigration.dtaddto))
    df_immigration = df_immigration.select(df_immigration.cicid.cast(lt()),
                                           df_immigration.i94yr.cast(i()),
                                           df_immigration.i94mon.cast(i()),
                                           df_immigration.i94cit.cast(i()),
                                           df_immigration.i94res.cast(i()),
                                           df_immigration.i94port.cast(st()),
                                           df_immigration.arrdate,
                                           df_immigration.i94mode.cast(i()),
                                           df_immigration.i94addr.cast(st()),
                                           df_immigration.depdate,
                                           df_immigration.i94bir.cast(i()),
                                           df_immigration.i94visa.cast(i()),
                                           df_immigration.visapost.cast(st()),
                                           df_immigration.biryear.cast(i()),
                                           df_immigration.dtaddto,
                                           df_immigration.gender.cast(st()),
                                           df_immigration.airline.cast(st()),
                                           df_immigration.admnum.cast(lt()),
                                           df_immigration.fltno.cast(st()),
                                           df_immigration.visatype.cast(st()))                                                                  
     
    df_mode = pd.read_csv('I94mode.txt', sep=" = ", header=None, engine='python',  names = ["transport_code", "mode"], skipinitialspace = True)
    df_mode["mode"] = df_mode["mode"].str.replace("'","")
    df_mode = spark.createDataFrame(df_mode)

    df_visa = pd.read_csv('I94Visa.txt', sep=" = ", header=None, engine='python',  names = ["visa_code", "visa"], dtype = {"visa_code": 'int64', "visa": "object"}, skipinitialspace = True)
    df_visa = spark.createDataFrame(df_visa)
                                                                  
    df_immigration.createOrReplaceTempView("immigration_table")
    df_mode.createOrReplaceTempView("mode_table")
    df_visa.createOrReplaceTempView("visa_table")
    df_immigration_final = spark.sql("""select i.cicid as cic_id,
                                           i.i94yr as year_of_entry,
                                           i.i94mon as month_of_entry,
                                           i.i94cit as country_of_citizenship,
                                           i.i94res as country_of_residence,
                                           i.i94port as port_of_entry,
                                           i.arrdate as arrival_date,
                                           m.mode as arrival_mode,
                                           i.i94addr as us_state,
                                           i.depdate as departure_date,
                                           i.i94bir as person_age,
                                           v.visa as visa_code,
                                           i.visapost as visa_issuing_post,
                                           i.biryear as person_birth_year,
                                           i.dtaddto as person_allowed_date,
                                           i.gender as gender,
                                           i.airline as airline,
                                           i.admnum as admission_number,
                                           i.fltno as flight_number,
                                           i.visatype as visa_type
                                           from immigration_table i left join visa_table v on i.i94visa==v.visa_code
                                           left join mode_table m on i.i94mode==m.transport_code
                                           """)
    df_immigration_final = df_immigration_final.filter(df_immigration_final.cic_id.isNotNull()).drop_duplicates(subset=['cic_id'])                                                             
    df_immigration_final.write.mode("overwrite").partitionBy("year_of_entry", "month_of_entry", "visa_type").parquet(output_data + "visatype/visatype.parquet")
    df_immigration_final.write.mode("overwrite").partitionBy("arrival_date").parquet(output_data + "arrival_date/poe.parquet")
    df_immigration_final.write.mode("overwrite").partitionBy("year_of_entry", "month_of_entry","country_of_citizenship").parquet(output_data + "citizenship/citizenship.parquet")                                                                 
def main():
    spark = create_spark_session()
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    KEY=config['AWS']['AWS_ACCESS_KEY_ID']
    SECRET=config['AWS']['AWS_SECRET_ACCESS_KEY']
    bucket = config['AWS']['BUCKET']
    
    try:
        s3_client = boto3.client('s3', 
                                 region_name='us-west-2', 
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)
        location = {'LocationConstraint': 'us-west-2'}
        s3_client.create_bucket(Bucket=bucket,
                                CreateBucketConfiguration=location)
    except ClientError as e:
        print(e)
    output_data = "analytics/"
    
    print("####PROCESSING AIRPORT CODE TABLE#####")
    airport_codes_processing(spark, 'airport-codes_csv.csv', output_data)
    print("####PROCESSING US CITY DEMOGRAPHICS TABLE")
    city_demographics_processing(spark, 'us-cities-demographics.csv', output_data)
    print("####PROCESSING COUNTRY TABLE")
    country_table_processing(spark, 'I94cntyl.txt', output_data)
    print("####PROCESSING PORT OF ENTRY TABLE")
    port_of_entry_processing(spark, 'I94port.txt', output_data) 
    print("####PROCESSING STATE TABLE")
    state_table_processing(spark, 'I94State.txt', output_data)
    print("####PROCESSING IMMIGRATION TABLE")
    immigration_table_processing(spark, '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', output_data)               
    
    for subdir, dirs, files in os.walk(output_data):
        for file in files:
            basepath = os.path.join(subdir, file)
            try:
                s3_client.put_object(Bucket=bucket, Key=(subdir+'/'))
                s3_client.upload_file(basepath, bucket, basepath)
            except ClientError as e:
                print(e)
    
if __name__ == "__main__":
    main()