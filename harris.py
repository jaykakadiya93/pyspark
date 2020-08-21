# Databricks notebook source
# DBTITLE 1,Readme
"""
All county notebooks should expose the public functions listed below.  The values returned by the functions will be serialized to JSON before they're returned to the calling notebook. The notebook below is an example using the bexar county dataset.

public functions:
  ** get_county_vars() **
  Returns a dict of the following county specific vars that will be added to the bronze and silver tables as metadata and/or used elsewhere in the script:
    1 - file_patterns:dict - A list of unix glob patterns that should be present in the dataset dir in order to perform transforms. The key names should be 
                              descriptive as they are used in other parts of the script to reference a paticualar datasource within a dataset.
                              Example: { "src1": "*some_file_0?.txt", "src2": "*some_other_file.json*" }
    2 - county:string - The County name, in lower case and separated with a dash "-" if the made up of more than one word.
    3 - state:string - The two character state abbreviation in uppercase.
    4 - fips_code:string - The five digit fips code.
  
  
  ** extract(args:dict) **
  Takes a nested dict comprised of data sources and destinations. This method should perform the following actions:
    1 - Read the data source using the src value of each key and prepare the source to be loaded into a table, keeping as close to the original format as possible
    2 - Create a global temp table
    3 - Returns a list of the table names that were created.  The table names should have the same name as the top level dict key
    Example args: { "src1": {"src1": "/path/to/src/file1", "dst": "..."}, "src2": {"src2": "/path/to/src/file2", "dst": "..."} }
    Example create table: spark.read.text(src).createOrReplaceGlobalTempView(name)
    
    
    
  ** transform(args:list) **
  Takes a list of temp table names.  This method should perform the following actions:
    1 - Perform transforms
    2 - Merge dataframes (if needed)
    3 - Create a temp table
    4 - Return the name of the temp table as a string
""" 

# COMMAND ----------

def get_county_vars():
  """
  The top level key value pairs will be added as columns to the dataset.
  The "file_patterns" key value pairs determine which files must be present 
  in the source dataset in order to perform the transform
  """
  county_vars = {
    "county": "harris",
    "state": "TX",
    "fips_code": "48201",
    "file_patterns": {
      "real_acct": "*real_acct.txt*",
      "building_other": "*building_other.txt*",
      "building_res": "*building_res.txt*",
      "fixtures": "*fixtures.txt*"
    }    
  }
  
  return county_vars

# COMMAND ----------

from pathlib import Path

def extract(args:dict):
  """ 
  Prepeares raw dataset to be loaded into bronze table keeping as close to originale as possible.
  Iterate over each key in file_patterns, perform any necassary cleaning steps and extract the the 
  file based on it's file type.  The resulting dataframe should be copied to a GlobalTempView .  The 
  name of the GlobalTempView should be added to the tables list.
  """
  
  tables = []
  
  for k in args.keys():
    name = k
    src = args[k]["src"]
    
    # all files in this dataset use the same options      
    spark.read.option("sep", "\t").csv(src).createOrReplaceGlobalTempView(name)
    tables.append(name)

  return tables

# COMMAND ----------

def _transform_fixtures(table_name):
  
  fixtures = spark.sql(f"""SELECT * FROM {table_name}""")
  # loading rooms from fixtures
  fixtures_rooms = fixtures \
    .select(F.trim(F.col("_c0")).alias("_c0"),F.trim(F.col("_c2")).alias("_c2"),F.col("_c4")) \
    .filter(F.col("_c2") == F.lit("RMT")) \
    .groupBy("_c0").agg(
        F.trim(F.col("_c0")), F.sum("_c4").alias("_c4")
    ) \
    .select(F.col("_c0").alias("parcel_id"),F.col("_c4").cast("integer").alias("rooms"))
  
  # loading bedrooms from fixtures
  fixtures_bedrooms = fixtures \
    .select(F.trim(F.col("_c0")).alias("_c0"),F.trim(F.col("_c2")).alias("_c2"),F.col("_c4")) \
    .filter(F.col("_c2") == F.lit("RMB")) \
    .groupBy("_c0").agg(
        F.trim(F.col("_c0")), F.sum("_c4").alias("_c4")
    ) \
    .select(F.col("_c0").alias("parcel_id"),F.col("_c4").cast("integer").alias("bedrooms"))
  
  # loading full bathrooms from fixtures
  fixtures_whole_bathooms = fixtures \
    .select(F.trim(F.col("_c0")).alias("_c0"),F.trim(F.col("_c2")).alias("_c2"),F.col("_c4")) \
    .filter(F.col("_c2") == F.lit("RMF")) \
    .groupBy("_c0").agg(
        F.trim(F.col("_c0")), F.sum("_c4").alias("_c4")
    ) \
    .select(F.col("_c0").alias("parcel_id"),F.col("_c4").cast("integer").alias("whole_bath"))
  
  # loading half bathrooms from fixtures
  fixtures_half_bathrooms = fixtures \
    .select(F.trim(F.col("_c0")).alias("_c0"),F.trim(F.col("_c2")).alias("_c2"),F.col("_c4")) \
    .filter(F.col("_c2") == F.lit("RMH")) \
    .groupBy("_c0").agg(
        F.trim(F.col("_c0")), F.sum("_c4").alias("_c4")
    ) \
    .select(F.col("_c0").alias("parcel_id"),F.col("_c4").cast("integer").alias("half_bath"))
  
  # joining fixtures
  fixtures_all_rooms = fixtures_rooms.join(
    fixtures_bedrooms, "parcel_id", "left").join(
        fixtures_whole_bathooms, "parcel_id", "left").join(
            fixtures_half_bathrooms, "parcel_id", "left"
            ).fillna({"half_bath": 0})

  rooms = fixtures_all_rooms \
    .withColumn("bathrooms", (F.col("whole_bath") + (F.col("half_bath").cast("float") * 0.5)))
  
  return rooms

# COMMAND ----------

def _transform_building_res(table_name):
  return (
    (spark.sql(f"""SELECT * FROM {table_name}"""))
    .select(
      F.trim(F.col("_c0")).alias("parcel_id"),
      F.col("_c19").cast("integer").alias("structure_total_sqft_res"),
    )
  )

# COMMAND ----------

def _transform_building_other(table_name):
  return (
    (spark.sql(f"""SELECT * FROM {table_name}"""))
    .groupBy("_c0").agg(
      F.trim(F.col("_c0")), F.sum("_c21").alias("structure_total_sqft_other"),
      F.trim(F.col("_c0")), F.max("_c32").alias("unit_count")
      ).select(
        F.trim(F.col("_c0")).alias("parcel_id"),
        F.col("structure_total_sqft_other").cast("integer"),
        F.col("unit_count").cast("integer"),
      )
  )

# COMMAND ----------

def _transform_real_acct(table_name):
  return (
    (spark.sql(f"""SELECT * FROM {table_name}"""))
    .select(
      F.col("_c2"),F.col("_c3"),F.col("_c4"),F.col("_c5"),
      F.col("_c6"),F.col("_c7"),F.col("_c8"),
      F.col("_c10"),
      F.col("_c12"),
      F.col("_c13"),
      F.col("_c14"),
      F.col("_c66"),
      F.col("_c67"),
      F.col("_c68"),
      F.col("_c69"),
      F.trim(F.col("_c0")).alias("parcel_id"),
      F.trim(F.col("_c0")).alias("tax_id"),
      F.trim(F.col("_c11")).alias("street_number"),
      F.trim(F.col("_c15")).alias("street_post_direction"),
      F.trim(F.col("_c16")).alias("unit"),
      F.trim(F.col("_c17")).alias("street_address"),
      F.trim(F.col("_c18")).alias("city"),
      F.trim(F.col("_c19")).alias("zip"),
      F.trim(F.col("_c20")).alias("state_use_code"),
      F.col("_c33").cast("integer").alias("year_built"),
      F.col("_c40").cast("double").alias("lot_size_acres"), #was float, would double be better?
      F.col("_c43").cast("integer").alias("land_market_value"),
      F.col("_c44").cast("integer").alias("improvement_market_value"),
      F.col("_c49").cast("integer").alias("market_value"),
      F.col("_c47").cast("integer").alias("assessed_value"),
      # Must cast as date or timestamp for Mongo       
      F.col("_c65").cast("date").alias("deed_transfer_date"),
    )
    .withColumn("type", F.when(
      F.col("state_use_code").rlike("(^A|^B|C1$)"), "Residential").otherwise("Commercial"))
    .withColumn("lot_size_sqft", (F.col("lot_size_acres") * 43560).cast("integer"))
    .withColumn("street_name", F.trim(F.concat_ws(" ", "_c13", "_c14")))
    # _c10 and _c12 represent the same address field. _c12 must come before _c10
    .withColumn(
      "street_pre_direction",
      quinn.remove_all_whitespace(
        quinn.remove_non_word_characters(
          F.concat_ws(
            " ",
            "_c12",
            "_c10"
          )
        )
      )
    )
    .withColumn(
      "legal_description",
      F.concat_ws(
        " ",
        "_c66",
        "_c67",
        "_c68",
        "_c69"
      )
    )
    .withColumn(
        "oor",
        quinn.single_space(
            F.concat_ws(
                " ",
                "_c2",
                "_c3",
                "_c4",
                "_c5",
                "_c6",
                "_c7",
                "_c8"
            )
        )
    )
  ) \
.drop("_c2","_c3","_c4","_c5","_c6","_c7","_c8", "_c10", "_c12", "_c13", "_c14", "_c66", "_c67", "_c68", "_c69")

# COMMAND ----------

import quinn
from pyspark.sql import functions as F

def transform(args:list):
  """
  Perform all necassary transforms, join the data if necessary and create a GlobalTemView with the final result.
  The table name should be the same as the County name. The actual transformation steps should be defined as a 
  separate function.  This function will serve as the orchestrator.
  """
  global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
  
  for name in args:
    if name == "real_acct":
      table_name = global_temp_db + "." + name
      real_acct = _transform_real_acct(table_name)
    elif name == "building_other":
      table_name = global_temp_db + "." + name
      building_other = _transform_building_other(table_name)
    elif name == "building_res":
      table_name = global_temp_db + "." + name
      building_res = _transform_building_res(table_name)
    elif name == "fixtures":
      table_name = global_temp_db + "." + name
      fixtures = _transform_fixtures(table_name)
    else:
      raise NameError()
  
  # create temp table from merged dataframes
  table = "harris"
  real_acct \
    .join(building_other, "parcel_id", "left") \
    .join(building_res, "parcel_id", "left") \
    .join(fixtures, "parcel_id", "left") \
    .withColumn(
        "structure_total_sqft",
        F.when(F.col("structure_total_sqft_res").isNotNull(), F.col("structure_total_sqft_res")) \
        .when(F.col("structure_total_sqft_other").isNotNull(), F.col("structure_total_sqft_other"))
    ) \
    .drop("structure_total_sqft_res", "structure_total_sqft_other") \
    .createOrReplaceGlobalTempView(table)
  
  return table

# COMMAND ----------

# DBTITLE 1,Nothing here needs to be changed
# allows county specific actions to be called in dbrix notebook
import json

# list of valid public functions
functions = ["get_county_vars", "extract", "transform"]

# raise exception if the notebook is called without a fn
try:
  fn = dbutils.widgets.get("fn")
  # make sure fn is available
  if fn not in functions:
    raise NotImplementedError
except Exception as e:
  if 'com.databricks.dbutils_v1.InputWidgetNotDefined' in str(e):
    raise AttributeError(f"You must pass a fn parameter to this notebook.  Valid fn values are: {functions}. {e}")
  else:
    raise e
  

# get fn args if present
try:
  fnargs = dbutils.widgets.get("args")
  args = json.loads(fnargs)
except Exception as e:
  if 'com.databricks.dbutils_v1.InputWidgetNotDefined' in str(e):
    args = None
  else:
    raise e
    
# call function
if args:
  return_value = locals()[fn](args)
else:
  return_value = locals()[fn]()

# return as json
dbutils.notebook.exit(json.dumps(return_value))