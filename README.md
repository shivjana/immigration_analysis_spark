
# Immigration Analysis
### Data Engineering Capstone Project

#### Project Summary
This project involves performing extraction and transformation of huge datasets containing millions of rows and then loading it back for upstream users for analysis on the overall immigration that has happened over the last few years in USA

The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 

The datasets that are gathered from various sources are to be ultimately used for data analysis. Apache pyspark will be used for extracting data from local source and loading the files on to Amazon S3 while for the transformation process that involves data wrangling and optimization both pandas and pyspark will be used


#### Describe and Gather Data 
Following are the datasets that have been gathered:

1) I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is used for mapping the various codes to their corresponding values

2) U.S. City Demographic Data: This data comes from OpenSoft. This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey.

3) Airport Code Table: This is a simple table of airport codes and corresponding cities that comes from the public domain ourairports. 
Code Snippet for loading data

1) loading Immigration data

df_immigration = spark.read.format("com.github.saurfang.sas.spark").load(input_data)

2) loading Data dictionary

df_country = pd.read_csv('I94cntyl.txt', sep=" =  ", header=None, engine='python',  names = ["country_code", "country"], skipinitialspace = True)

df_port = pd.read_csv('I94port.txt', sep="	=	", header=None, engine='python',  names = ["port_of_entry", "place"], skipinitialspace = True)

df_state = pd.read_csv('I94State.txt', sep="=", header=None, engine='python',  names = ["state_code", "state"], skipinitialspace = True)

df_mode = pd.read_csv('I94mode.txt', sep=" = ", header=None, engine='python',  names = ["transport_code", "mode"], skipinitialspace = True)

df_visa = pd.read_csv('I94Visa.txt', sep=" = ", header=None, engine='python',  names = ["visa_code", "visa"], dtype = {"visa_code": 'int64', "visa": "object"}, skipinitialspace = True)

3) loading Airport code table

df_airport = spark.read\
                     .format("csv")\
                     .option("header", "true")\
                     .load("airport-codes_csv.csv")
                     
4) loading Demographics table

demographics_schema = StructType ([
    sf("City",st()),
    sf("State", st()),
    sf("Median Age", dec()),
    sf("Male Population", i()),
    sf("Female Population", i()),
    sf("Total Population", i()),
    sf("Number of Veterans", i()),
    sf("Foreign-born", i()),
    sf("Average Household Size", dec()),
    sf("State Code", st()),
    sf("Race", st()),
    sf("Count", i())
])

df_demographics = spark.read.csv('us-cities-demographics.csv', header='true', sep=";", schema=demographics_schema)

### Step 2: Explore and Assess the Data

As part of identifying data quality issues, each of the source files are explored for any missing or duplicate or invalid values that don't conform to the schema defined. Following are some of the issues that are identifed:

1) Country Codes

The country codes and their corresponding values which are country names consist of values which are unknown or invalid based on data dictionary definition. This is resolved by replacing those ones as 'NA'. All values containing extra quotes have been removed. 

df_country["country"] = df_country["country"].replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*"], value="NA", regex=True)

df_country["country"] = df_country["country"].str.replace("'","")

2) Port of entry codes

The port of entry codes and their corresponding values which are port names consist of unknown or invalid values based on data dictionay definition. This is resolved by replacing those ones as 'NA'. Extra quotes present in the values have been removed.

df_port["place"] = df_port["place"].replace(to_replace=["Collapsed.*", "No PORT Code.*"], value="NA", regex=True)

df_port["place"] = df_port["place"].str.replace("'","")

df_port["port_of_entry"] = df_port["port_of_entry"].str.replace("'","")

3) State codes

The state codes and their corresponding values which are state names consist of tab and quote characters. They are stripped off from the values.

df_state['state_code'] = df_state['state_code'].str.strip('\t')

df_state["state_code"] = df_state["state_code"].str.replace("'","")

df_state["state"] = df_state["state"].str.replace("'","")

4) Immigration table

The dates namely arrival date, departure date and the date of admission are not in proper date format. Missing values have been removed from these columns and values containing valid characters have been stripped off. All the columns are not in the right data type and so have been casted to the right data type

calDate = udf(lambda t: datetime.datetime.fromtimestamp(int(float(t))).strftime("%x"))

dateType =  udf (lambda x: datetime.datetime.strptime(x, '%m/%d/%y'), da())

df_immigration = df_immigration.withColumn('arrdate',calDate(df_immigration.arrdate))

df_immigration = df_immigration.withColumn('arrdate',dateType(df_immigration.arrdate))

df_immigration = df_immigration.filter(df_immigration.depdate.isNotNull()).withColumn('depdate',calDate(df_immigration.depdate))

df_immigration = df_immigration.withColumn('depdate',dateType(df_immigration.depdate))

remNonAlphaChars = udf(lambda s:re.sub(r'\W+', '', s))

df_immigration = df_immigration.filter(df_immigration.dtaddto.isNotNull()).withColumn('dtaddto',remNonAlphaChars(df_immigration.dtaddto))

splitDate = udf(lambda s: s[:2] + "/" + s[2:4] + "/" + s[4:] if s.lower().islower() == False and len(s) == 8 else "INVALID")

date4Type =  udf (lambda x: datetime.datetime.strptime(x, '%m/%d/%Y'), DateType())

df_immigration = df_immigration.withColumn('dtaddto',splitDate(df_immigration.dtaddto))

df_immigration = df_immigration.where(df_immigration.dtaddto != "INVALID")

df_immigration = df_immigration.withColumn('dtaddto',date4Type(df_immigration.dtaddto))

df_immigration = df_immigration.select(df_immigration.cicid.cast(LongType()),
                                       df_immigration.i94yr.cast(IntegerType()),
                                       df_immigration.i94mon.cast(IntegerType()),
                                       df_immigration.i94cit.cast(IntegerType()),
                                       df_immigration.i94res.cast(IntegerType()),
                                       df_immigration.i94port.cast(StringType()),
                                       df_immigration.arrdate,
                                       df_immigration.i94mode.cast(IntegerType()),
                                       df_immigration.i94addr.cast(StringType()),
                                       df_immigration.depdate,
                                       df_immigration.i94bir.cast(IntegerType()),
                                       df_immigration.i94visa.cast(IntegerType()),
                                       df_immigration.visapost.cast(StringType()),
                                       df_immigration.biryear.cast(IntegerType()),
                                       df_immigration.dtaddto,
                                       df_immigration.gender.cast(StringType()),
                                       df_immigration.airline.cast(StringType()),
                                       df_immigration.admnum.cast(LongType()),
                                       df_immigration.fltno.cast(StringType()),
                                       df_immigration.visatype.cast(StringType()))   

5) Airport Codes table

The column iso-region which corresponds to states in a country is represented as 'country_code-state_code', for eg, 'US-CA', instead of just 'CA'. Since we need only the state codes for the column iso-region, other characters are stripped off and only the state codes are present in the values of iso-region. Another issue that identified is that iata_code and local_code have lot of null values. Since both columns mean the same, they are merged in to a column port_code which will represent the airport codes and all duplicates and null values are removed.

col1 = ['ident','type','name','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','iata_code','coordinates']

col2 = ['ident','type','name','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','local_code','coordinates']

df_1_new = df_airport.filter("iso_country == 'US'").select(col1).where(df_airport.iata_code.isNotNull())

df_1_new = df_1_new.withColumnRenamed('iata_code','port_code')

df_2_new = df_airport.filter("iso_country == 'US'").select(col2).where(df_airport.local_code.isNotNull())

df_2_new = df_2_new.withColumnRenamed('local_code','port_code')

df_airport = df_1_new.union(df_2_new)

df_airport = df_airport.drop_duplicates(subset=['port_code'])

df_airport = df_airport.withColumn('state', split('iso_region',"-")[1]).drop('iso_region')

6) Demographics table

The demographics table contain columns with column names that have white spaces and also the data types for each of them are not proper. So a schema is defined such that each of the column names with whitespaces are renamed and proper data types are defined before loading the file from the source.

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

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model


```python
from IPython.display import Image
Image(filename='model_design.jpg')
```




![jpeg](output_6_0.jpeg)



#### 3.2 Mapping Out Data Pipelines

1) Process and clean airports codes data and then save them to parquet files partitioned by states

2) Process and clean us demographics data and then save them to parquet files partitioned by state and city

3) Process and clean the country codes data from i94cntyl.txt and then save them to parquet files partitioned by the country

4) Process and clean the US state codes data from i94State.txt and then save them to parquet files partitioned by the states

5) Process and clean the us ports data from i94port.txt and then save them to parquet files partitioned by the cities which are the port of entries

6) Process and clean the immigration data which are then later joined with visa codes and transport modes from the visa table and transport mode table derived from data dictionary. This is then saved in parquet files paritioned by i) year_of_entry, month_of_entry and visa types. ii) arrival_date iii) year_of_entry, month_of_entry, country_of_citizenship

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Following is the way to run the pipeline:

python etl.py
#### 4.2 Data Quality Checks
1) Immigration table

    i) Ensure the data types are expected 
        df_immigration_final.printSchema()
   ii) Ensure primary key cic_id has no null values and there are no duplicates
        df_immigration_final.cic_id.isNotNull()
        df_immigration_final.drop_duplicates(subset=['cic_id'])
   ii) Ensure the count of rows are as expected
        df_immigration_final.count()
        
2) Airport table

    i) Ensure the data types are expected 
        df_airport.printSchema()
   ii) Ensure primary key port_code has no null values and there are no duplicates
        df_airport.filter(df_airport.port_code.isNotNull()).drop_duplicates(subset=['port_code'])
   ii) Ensure the count of rows are as expected
        df_airport.count() 
        
3) US City demographics table

    i) Ensure the data types are expected 
        df_demographics.printSchema()
   ii) Ensure primary keys race, city and state has no null values and there are no duplicates
        df_demographics = df_demographics.filter(df_demographics.State.isNotNull()).drop_duplicates(subset=['City','State', 'Race'])
   ii) Ensure the count of rows are as expected
        df_demographics.count()
        
4) State table

    i) Ensure the data types are expected 
        df_state.printSchema()
   ii) Ensure primary key state_code has no null values and there are no duplicates
        df_state.filter(df_state.state_code.isNotNull()).drop_duplicates(subset=['state_code'])
   ii) Ensure the count of rows are as expected
        df_state.count() 
        
5) Country table

    i) Ensure the data types are expected 
        df_country.printSchema()
   ii) Ensure primary key country_code has no null values and there are no duplicates
        df_country.filter(df_country.country_code.isNotNull()).drop_duplicates(subset=['country_code'])
   ii) Ensure the count of rows are as expected
        df_country.count()
        
6) Port of Entry table

    i) Ensure the data types are expected 
        df_port.printSchema()
   ii) Ensure primary key port_of_entry has no null values and there are no duplicates
        df_port.filter(df_port.port_of_entry.isNotNull()).drop_duplicates(subset=['port_of_entry'])
   ii) Ensure the count of rows are as expected
        df_port.count()
#### 4.3 Data dictionary 

1) The description of data fields for the airport table is as described in below link:
https://ourairports.com/help/data-dictionary.html

2) The description of data fields for the immigration table, state table, country table and port of entry tableare in the file I94_SAS_Labels_Descriptions.SAS

3) The description of data fields for the us demographics table is as follows:

City - City where survey was performed

State - State in which the city resides

Median Age - Median age of the people in the City

Male Population - total male population in the City

Female Population - total female population in the city

Total population - total population in the city

Number of veterans - Total number of veterans in the City

Foreign-born - Total number of foreign-born in the City

Average Household size - Average size of the household in the City

State Code - state code for the State in which the city resides

Race - race by which population is calculated in the city

Count - total number of records

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.


```python

```


```python

```


```python

```


```python

```


```python

```


```python

```
