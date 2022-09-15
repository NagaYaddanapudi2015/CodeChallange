# Databricks notebook source
date_time_str = '09/09/22 01:55:19'

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *
from datetime import datetime
import random,logging,uuid
import numpy as np
import pandas as pd

# COMMAND ----------

def check_if_valid_date(date: str):
    """
    Verify that the date format matches d/m/y
    :param date: str date in d/m/y format
    :return: True or False
    """
    date_format = "%d/%m/%Y"

    """ Warning to any non python devs reading this code..
        In Python the only way to test a valid date is with a try catch. Yep, it sux.
    """
    if not isinstance(date, str):
        return False

    try:
        datetime.strptime(date, date_format)
        valid_date = True
    except ValueError:
        valid_date = False

    return valid_date


def random_nan(x):
    """
    Replace x with a nan, if the random number == 1
    """
    if random.randrange(0, 15) == 1:
        x = np.nan

    return x


def generate_new_random_trade_position(date: str):
    """ Generates a new random trade position with the date, period sequence and volume sequence
    :param date: Date in d/m/y format
    :return: dict with data
    """

    period_list = [random_nan(i.strftime("%H:%M")) for i in pd.date_range("00:00", "23:59", freq="5min").time]
    volume = [random_nan(x) for x in random.sample(range(0, 500), len(period_list))]

    open_trade_position = {"date": date,
                           "time": period_list,
                           "volume": volume,
                           "id": uuid.uuid4().hex
                           }

    return open_trade_position


def get_trades(date: str):
    """
    Generate some random number of open trade positions
    :param date: date in d/m/y format
    :return:
    """

    if not check_if_valid_date(date=date):
        error_msg = "The supplied date {} is invalid.Please supply a date in the format d/m/Y.".format(date)
        logging.error(error_msg)
        raise ValueError(error_msg)

    # a randomly chosen number of open trades
    number_of_open_trades = random.randint(1, 101)
    logging.info("Generated" + str(number_of_open_trades) + " open trades randomly.")

    open_trades_list = []
    # Generate a list of open trade dicts
    for open_trade in range(0, number_of_open_trades):
        open_trades_list.append(generate_new_random_trade_position(date=date))

    return open_trades_list

# COMMAND ----------

def saveResult(dataframe,temp_loc,file_path):
    dataframe.write.mode('append').csv(temp_location)
    file =dbutils.fsl.s(temp_location)[-1].path
    dbutils.fs.cp(file,file_path)
    dbutils.fs.rm(temp_location,recurse=True)
    
def log_data_processed(Data_Source :str,message:str,env:str):
        """_summary_
                Log successful runs to report on data freshness
            Args:
                Data_Source : Source System
                message: DQ message or Run Successfull message
            Returns:
                new record in logging table
            """
        try:
            Id = str(uuid.uuid4())
            Update_Date = datetime.now()
            table_name = 'Processed_Date'
            sink_path = f"abfss://logging@{env}/custom_logs/{table_name}"
            # create explicit schema
            schema = StructType([ \
                StructField("Id",StringType(),True), \
                StructField("Data_Source",StringType(),True), \
                StructField("message",StringType(),True)
            ]) 
            #create data payload list
            dataset = [(Id,Data_Source,message)]
            #promote list to pyspark dataframe
            staging_df = spark.createDataFrame(dataset, schema)
            staging_df.write.mode('append').save(sink_path + '/delta')
            
        except Exception as e:
            print(f"Error in log_data_processed function: {e}")

from pyspark.sql.utils import AnalysisException
from great_expectations.dataset import SparkDFDataset


data_quality_message = ''

def check_mand_cols(agg_dq_df):
    for column in mand_cols:
        try:
            assert agg_dq_df.expect_column_to_exist(column).success, f"Error! Mandatory column {column} does not exist: FAILED"
            return f"Column {column} exists : PASSED   "
        except AssertionError as e:
            return str(e)

def check_not_null_cols(agg_dq_df):
    for column in mand_cols:
        try:
            test_result = agg_dq_df.expect_column_values_to_not_be_null(column)
            assert test_result.success, \
                f"{test_result.result['unexpected_count']} of {test_result.result['element_count']} items in column {column} are null: FAILED  "
            return f"All items in column {column} are not null: PASSED  "
        except AssertionError as e:
            return str(e) 
        except AnalysisException as e:
            return str(e)

# COMMAND ----------

input_date = datetime.strptime(date_time_str, '%d/%m/%y %H:%M:%S').strftime('%d/%m/%Y')
trades = get_trades(date=input_date)
df = pd.DataFrame(trades[0])
## convert pandas dataframe to Spark DataFrame
sparkDF=spark.createDataFrame(df)
## handling na and null values by dropping rows with na
sparkDF=sparkDF.na.drop()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

agg_hr = sparkDF.groupBy(F.window("time","1 hour", "1 hour", "0 minutes").alias("hour_timestamp_window"))\
                .agg(F.sum("volume").alias("volume"))\
                .orderBy(F.col("hour_timestamp_window").desc())\
                .withColumn("localtime", F.date_format(F.to_timestamp("hour_timestamp_window.start","dd/MM/yyyy HH:mm:ss"),"HH:mm")).drop("hour_timestamp_window")\
                .select("localtime","volume")

# COMMAND ----------

agg_dq_df = SparkDFDataset(sparkDF)
mand_cols = ["date","time","volume","id"]

data_quality_message = check_mand_cols(agg_dq_df)
data_quality_message+= check_not_null_cols(agg_dq_df)

# COMMAND ----------

print(data_quality_message)

# COMMAND ----------

log_data_processed("DQ Test" ,data_quality_message,"dev")

# COMMAND ----------

date_time_obj = datetime.strptime(date_time_str, '%d/%m/%y %H:%M:%S')
filename = "/mnt/processed/"+"PowerPosition_"+date_time_obj.strftime("%y%m%d_%H%M")+".csv"
temp_loc = "/mnt/temp"

# COMMAND ----------

## persists data to a temp location and while moving file from temp location to actual hdfs/datalake path
## as spark executor doesn't support named output formats
saveResult(agg_hr,temp_loc,filename)

# COMMAND ----------

display(agg_hr)
