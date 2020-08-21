# Databricks notebook source
# DBTITLE 1,Function definitions are defined before parsing args
"""
All county notebooks should expose the public functions listed below.  The values returned by the functions will be serialized to JSON before they're returned to the calling notebook.

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
  county_vars = {
    "county": "bexar",
    "state": "TX",
    "fips_code": "48029",
    "file_patterns": {
      "pse": "*Property Summary Export*.csv",
      "ade_info": "*APPRAISAL_INFO.TXT",
      "ade_land": "*APPRAISAL_LAND_DETAIL.TXT",
    }    
  }
  
  return county_vars

# COMMAND ----------

from pathlib import Path

def extract(args:dict):
  
  tables = []
  
  """ prepeares raw dataset to be loaded into bronze table keeping as close to originale as possible """
  for k in args.keys():
    name = k
    src = args[k]["src"]
    bronze = args[k]["bronze"]
    
    if (name == 'ade_info' or name == 'ade_land'):
      """ reads dataset in as text and returns temp view """
      # df = spark.read.text(src).write.format("delta").mode("overwrite").save(bronze)
      spark.read.text(src).createOrReplaceGlobalTempView(name)
      tables.append(name)
      
    elif name == 'pse':
      p = Path(src)
      clean_pse = f"{p.parent}/clean_pse.csv"

      with open('/dbfs' + src, "rt") as s:
        with open('/dbfs' + clean_pse, "wt") as d:
          for line in s:
            d.write(line.replace('"""', '"'))
            
      # spark.read.csv(clean_pse, inferSchema=False, header=None).write.format("delta").mode("overwrite").save(bronze)
      spark.read.csv(clean_pse, inferSchema=False, header=None).createOrReplaceGlobalTempView(name)
      tables.append(name)

  return tables

# COMMAND ----------

def _transform_pse(table_name):
  return (
    (spark.sql(f"""SELECT 
    _c0, _c2, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c13, _c14, _c18, _c19, _c21, _c23, _c24, _c25, _c26, _c27, _c31, _c32, _c33, _c34, _c35, _c36, _c37, _c40, _c41, _c42, _c43, _c44, _c45 FROM {table_name}"""))
    .withColumn("flag", F.when(F.col("_c33").rlike("[A-z]"), True)) # columns have shifted
    .select(  # columns have shifted
          F.col("flag"),
          F.col("_c0").alias("parcel_id"),
          F.col("_c4"), F.col("_c5"), F.col("_c6"), F.col("_c7"), F.col("_c8"), F.col("_c9"), F.col("_c10"), # owner info
          F.col("_c13").cast("integer").alias("year_built"),
          F.col("_c14").cast("integer").alias("structure_total_sqft"),
          # F.col("_c15").cast("double").alias("lot_size_acres"),
          F.col("_c18").alias("state_use_code"),
          F.col("_c19").cast("integer").alias("market_value"),
          F.trim(F.col("_c23")).alias("street_number"),
          F.trim(F.col("_c24")).alias("street_pre_direction"),
          F.trim(F.col("_c25")).alias("street_name"),
          F.trim(F.col("_c26")).alias("street_post_direction"),
          F.trim(F.col("_c27")).alias("unit"),
          F.trim(F.col("_c31")).alias("roof_material"),
          F.trim(F.col("_c32")).alias("foundation"),
          F.col("_c36").cast("integer").alias("improvement_market_value"),
          F.col("_c37").cast("integer").alias("land_market_value"),
          F.col("_c40").alias("municipal_use_code"),
          F.col("_c41").alias("legal_subdivision"),
          F.col("_c43").alias("zip"),
          F.col("_c44").cast("integer").alias("unit_count"),
          # columns that require formatting
          F.regexp_replace(F.col("_c2"), "-", "").alias("tax_id"),
          F.regexp_replace(F.col("_c21"), "CITY OF ", "").alias("city"),
          F.regexp_extract(F.col("_c33"), r"(\d+)$", 1).cast("integer").alias("bedrooms"),
          F.regexp_extract(F.col("_c34"), r"(\d+)$", 1).cast("integer").alias("whole_bath"),
          F.regexp_extract(F.col("_c35"), r"(\d+)$", 1).cast("integer").alias("half_bath"),
          F.trim(quinn.single_space("_c42")).alias("legal_description"),
          # timestamp required for mongo
          F.to_timestamp(F.col("_c45"), "MM/dd/yyyy").alias("deed_transfer_date"),
      )
      .drop_duplicates(["parcel_id"])
      # derived columns
      .withColumn(
          "bathrooms",
          (F.col("whole_bath") + (F.col("half_bath").cast("float") * 0.5)),
      )
      .withColumn(
          "street_address",
          quinn.single_space(
              F.regexp_replace(
                  F.concat_ws(
                      " ",
                      "street_number",
                      "street_pre_direction",
                      "street_name",
                      "street_post_direction",
                      "unit",
                  ),
                  '"',
                  "",  # regexp replacement values
              )
          ),
      )
      .withColumn(
          "oor",
          quinn.single_space(
              F.concat_ws(
                  " ",
                  "_c4",
                  "_c5",
                  "_c6",
                  "_c7",
                  "_c8",
                  "_c9",
                  "_c10"
              )
          )
      )
  ).drop("_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10")

# COMMAND ----------

def _transform_info(table_name):
  return (
    (spark.sql(f"""SELECT * FROM {table_name}"""))
    .select(
          F.trim(F.col("value").substr(1, 12))
          .cast("integer")
          .cast("string")
          .alias("parcel_id"),  # prop_id
          F.trim(F.col("value").substr(13, 5)).alias("type"),  # prop_type_cd
#           F.trim(F.col("value").substr(2742, 10)).alias(
#               "land_use_code"
#           ),  # land_state_cd
          F.trim(F.col("value").substr(2732, 10)).alias(
              "improvement_use_code"
          ),  # imprv_state_cd
          F.trim(F.col("value").substr(1946, 15))
          .cast("integer")
          .alias("assessed_value"),  # assessed_val
      )
      .drop_duplicates(["parcel_id"])
      .where("type='R'")
  )

# COMMAND ----------

def _transform_land(table_name):
  return (
      (spark.sql(f"""SELECT * FROM {table_name}"""))
      .select(
          F.trim(F.col("value").substr(1, 12))
          .cast("integer")
          .cast("string")
          .alias("parcel_id"),
          F.trim(F.col("value").substr(29, 10)).alias("land_use_code"),
          F.trim(F.col("value").substr(39, 25)).alias("land_use_desc"),
          (F.trim(F.col("value").substr(70, 14))
          .cast("double") / 10000)
          .alias("lot_size_acres"),
          F.trim(F.col("value").substr(84, 14))
          .cast("integer")
          .alias("lot_size_sqft"),
          F.trim(F.col("value").substr(98, 14))
          .cast("integer")
          .alias("lot_frontage_ft"),
          F.trim(F.col("value").substr(112, 14))
          .cast("integer")
          .alias("lot_depth_ft"),
      )
      .drop_duplicates(["parcel_id"])
  )


# COMMAND ----------

# import quinn
# from pyspark.sql import functions as F

# pse = _transform_pse("global_temp.pse")
# info = _transform_info("global_temp.ade_info")
# land = _transform_land("global_temp.ade_land")

# df = info.join(pse, "parcel_id", "left").join(land, "parcel_id", "left")
# df.count()

# COMMAND ----------

import quinn
from pyspark.sql import functions as F

def transform(args:list):
  global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
  
  for name in args:
    if name == "pse":
      table_name = global_temp_db + "." + name
      pse = _transform_pse(table_name)
    elif name == "ade_info":
      table_name = global_temp_db + "." + name
      info = _transform_info(table_name)
    elif name == "ade_land":
      table_name = global_temp_db + "." + name
      land = _transform_land(table_name)
    else:
      raise NameError()
  
  # create temp table from merged dataframes
  table = "bexar"
  info.join(pse, "parcel_id", "left").join(land, "parcel_id", "left").createOrReplaceGlobalTempView(table)
  
  return table

# COMMAND ----------

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