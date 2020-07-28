import sys
import logging
import traceback
import configparser
import pyspark.sql.functions as func

from os import listdir
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

APP_HOME = sys.argv[1]
SRC_NM = sys.argv[2]
MATCH_TYPE = sys.argv[3]
SRC_LOOKUP_FILE = APP_HOME + "/SRC_LOOKUP_FILES/" + SRC_NM + "/" + MATCH_TYPE + "/" + sys.argv[4]
STAND_ADDR_FILE = APP_HOME + "/StandardizedAddressFiles/" + SRC_NM + "/" + sys.argv[5]
LOG_FILE = sys.argv[6]
PROPERTIES_FILE = APP_HOME + "/properties/config.properties"

DELIMITER = ";"
INC_DELIMITER = "|"
MAX_MEMORY = "10g"
LOGLEVEL = 'ERROR'
spark_temp_dir = APP_HOME + '/tmp'

# standardized address file has record_id and 5 standardized address fields.
try:
    # --------------------------------------------------------------------------------------------
    # logging.basicConfig(filename=APP_HOME + "/log.log", format='%(asctime)s %(message)s', filemode='w')
    # logger = logging.getLogger()
    start_time = datetime.now()
    print("\n--------------------------------------------")
    print(f"###### START @ {str(start_time)} ######")
    # --------------------------------------------------------------------------------------------
    config = configparser.RawConfigParser()
    config.read(PROPERTIES_FILE)
    SRC_UNIQUE_ID = config.get('SOURCE_UNIQUE_COLUMN', SRC_NM)

    # create Spark Session.
    # --------------------------------------------------------------------------------------------
    spark = SparkSession.builder.appName("Athena Merge Process").config("spark.executor.memory", '12g').config("spark.driver.memory", '6g').config("spark.memory.offHeap.enabled",'true').\
    config("spark.memory.offHeap.size","8g").config('spark.local.dir', spark_temp_dir).getOrCreate()


    # ignore SparkSession Warnings/Info messages on console.
    spark.sparkContext.setLogLevel(LOGLEVEL)
    # --------------------------------------------------------------------------------------------

    # read source mappings output file to DataFrame.
    # --------------------------------------------------------------------------------------------
    source_df1 = spark.read.option("header", "true").option("delimiter", DELIMITER).csv(SRC_LOOKUP_FILE)

    # drop columns from df1 so that these can be appended from standardized file.
    columns_to_drop = ['physical_address_street', 'physical_address_street2', 'physical_address_city', 'physical_address_state', 'physical_address_zip5']
    source_df1 = source_df1.drop(*columns_to_drop)

    print(f"\n\n###### Source Data Frame Record's Count Before Dups Drop: {str(source_df1.count())} ######")

    df1 = source_df1
    dups_df = df1.groupby("record_id").agg(count("record_id").alias("COUNT")).filter(col('COUNT') > 1)

    print(f"\n\n###### Source Duplicates Data Frame Record's Count: {str(dups_df.count())} ######")

    dups_df.coalesce(1).write.option("sep", DELIMITER).mode('overwrite').option("header", "true").csv(APP_HOME + "/Append_Standardization_Output/" + SRC_NM + "/" + MATCH_TYPE + "_duplicates_in_source_file")

    # *NOTE: drop is deleting few rows, not sure why will check later.
    # df1 = source_df1.drop_duplicates(['record_id'])

    print(f"\n\n###### Source Data Frame Record's Count After Dups Drop: {str(df1.count())} ######")
    # --------------------------------------------------------------------------------------------

    # --------------------------------------------------------------------------------------------
    # print DataFrame columns.
    # df1.printSchema()

    # print DataFrame.
    # df1.show()
    # --------------------------------------------------------------------------------------------

    # Appending Standardized Address from Standardized Address File.
    # --------------------------------------------------------------------------------------------
    df2 = spark.read.option("header", "true").option("delimiter", INC_DELIMITER).csv(STAND_ADDR_FILE)
    print(f"\n\n###### Standardized Addresses Data Frame Record's Count Before Dups Drop: {str(df2.count())} ######")

    df2 = df2.where(col("NAX_ADDRESS_LINE1").isNotNull())

    print(f"\n\n###### Standardized Addresses Data Frame Record's Count After Filter on NAX_ADDRESS_LINE1: {str(df2.count())} ######")

    df2 = df2.drop_duplicates([SRC_UNIQUE_ID])

    print(f"\n\n###### Standardized Addresses Data Frame Record's Count After Dups Drop: {str(df2.count())} ######")

    # join df1 and df2 based on record_id and select all columns from df1 and standardized address columns from df2.
    df3 = df1.alias('a').join(df2.alias('b'), col('a.record_id') == col('b.'+ SRC_UNIQUE_ID)).select([col('a.' + column) for column in df1.columns] + [col('b.NAX_ADDRESS_LINE1').alias('physical_address_street'), col('b.NAX_ADDRESS_LINE2').alias('physical_address_street2'), col('b.NAX_CITY').alias('physical_address_city'), col('b.NAX_STATE_CD').alias('physical_address_state'), col('b.NAX_ZIP_CODE').alias('physical_address_zip5')])

    print(f"\n\n###### Join Data Frame Record's Count: {str(df3.count())} ######")
    # --------------------------------------------------------------------------------------------

    # selecting columns to make the order of columns consistent for the generating hash.
    # --------------------------------------------------------------------------------------------
    df4 = df3.select('be_id', 'build_id', 'location_name_legal', 'physical_address_carrier_route', 'address',
              'physical_address_cass_certified', 'chain_count', 'chain_type', 'city', 'closed', 'closed_date',
              'custom_place_attributes', 'custom_place_attributes_value', 'physical_address_delivery_point',
              'typed_phone_numbers_do_not_call', 'physical_address_e_lot', 'location_headcount_int',
              'HASH_MD5_ADDR_MLNG', 'hq', 'physical_address_is_seasonal', 'physical_address_is_vacant',
              'location_headcount', 'location_revenue', 'mailable', 'mailing_address_street', 'mailing_address_city',
              'mailing_address_state', 'mailing_address_zip4', 'mailing_address_zip5', 'place_id', 'metro',
              'mailing_address_carrier_route', 'mailing_address_cass_certified', 'mailing_address_correction_footnote',
              'mailing_address_delivery_point', 'mailing_address_dnk', 'mailing_address_dnk_type',
              'mailing_address_dnm', 'mailing_address_dnm_type', 'mailing_address_dpv_match_code',
              'mailing_address_e_lot', 'mailing_address_is_seasonal', 'mailing_address_is_vacant',
              'mailing_address_ncoa_move_date', 'mailing_address_ncoa_move_footnote', 'mailing_address_ncoa_move_type',
              'mailing_address_street2', 'mailing_address_address_type', 'mailing_address_zip_type',
              'business_start_date', 'physical_address_ncoa_move_date', 'physical_address_ncoa_move_footnote',
              'physical_address_ncoa_move_type', 'typed_phone_numbers_phone_type', 'physical_address_address_type',
              'physical_address_street', 'physical_address_street2', 'physical_address_correction_footnote',
              'physical_address_dpv_match_code', 'physical_address_city', 'physical_address_state','physical_address_zip4',
              'physical_address_zip5', 'place_quality_indicator', 'postal_code', 'postal_code4', 'sic_codes',
              'typed_phone_numbers_rank', 'RANK_NUMBER', 'physical_address_dnk', 'physical_address_dnk_type',
              'physical_address_dnm', 'physical_address_dnm_type', 'custom_place_attributes_record_type',
              'location_revenue_int', 'sic_descr', 'soho', 'typed_phone_numbers_source',
              'custom_place_attributes_record_id', 'custom_place_attributes_source', 'SRC_NM', 'state',
              'typed_phone_numbers_phone_number', 'location_name_dba', 'twitter_account', 'physical_address_zip_type',
              'address_key', 'address_key_extended', 'NAME', 'record_id', 'LATITUDE', 'LONGITUDE')
    # --------------------------------------------------------------------------------------------

    # write df4 to output file and send it to generate hash columns.
    # --------------------------------------------------------------------------------------------
    # df4.coalesce(1).write.option("sep", DELIMITER).mode('overwrite').option("header", "true").csv(APP_HOME + "/dnb_with_standardized_address_01_13_2020_with_Abhi_File")
    df4.coalesce(1).write.option("sep", DELIMITER).mode('overwrite').option("header", "true").csv(APP_HOME + "/Append_Standardization_Output/" + SRC_NM + "/" + MATCH_TYPE + "_updated_with_standardized_address")

    # df4.show()

    print(f"\n\n###### Final Data Frame Record's Count: {str(df4.count())} ######")
    # --------------------------------------------------------------------------------------------

    # Fallouts
    # --------------------------------------------------------------------------------------------
    df_fallouts = df1.select('record_id').subtract(df4.select('record_id'))
    print(f"\n\n###### Fallouts Data Frame Record's Count: {str(df_fallouts.count())} ######")
    df_fallouts.coalesce(1).write.option("sep", DELIMITER).mode('overwrite').option("header", "true").csv(APP_HOME + "/Append_Standardization_Output/" + SRC_NM + "/" + MATCH_TYPE + "_append_standardized_address_fallouts")

    # --------------------------------------------------------------------------------------------

    # --------------------------------------------------------------------------------------------
    end_time = datetime.now()
    print(f"\n###### ENDED @ {str(end_time)} ######")
    print(f"###### Total Run Time: {str(end_time - start_time)} (H:MM:SS.ssssss) ######")
    print("\n--------------------------------------------")
    # --------------------------------------------------------------------------------------------

except Exception:
    traceback.print_exc(file=open(LOG_FILE,"a"))
    sys.exit(1)

sys.exit(0)
