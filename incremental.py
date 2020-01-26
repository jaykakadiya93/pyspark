###############################################
#                  pyspark                    #
#               author:jay kakadiya           #
#                                             #
###############################################
from pyspark.sql import SparkSession
import sys
import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import configparser
import sys
import traceback

app_home = sys.argv[1]
log_file = sys.argv[2]
previous_file = sys.argv[3]
new_file = sys.argv[4]
jobproperties = app_home+'/job/job.properties'
try:
    properties_file = app_home+'/properties/config.properties'
    config = configparser.RawConfigParser()
    config.read(properties_file)
    sep = config.get('IncrementalSection','sep')
    column = config.get('IncrementalSection','column').split(sep)
    column_with_ind=config.get('IncrementalSection','column').split(sep)
    column_with_ind.append("INCR_IND")
    key = config.get('IncrementalSection','key').split(sep)
    driver_memory=config.get('IncrementalSection','spark.driver.memory')
    business_name=config.get('IncrementalSection','business.name')
    file = open(jobproperties,"a")

    start_time=str(datetime.datetime.now())
    print ("start time =",str(datetime.datetime.now()))
    spark = SparkSession.builder.appName("Incremental Application").config("spark.some.config.option", "some-value").config("spark.local.dir","/opt/app/").config("spark.driver.memory", driver_memory).getOrCreate()
    print("session start")
    # create dataframe with insert INCR_IND from current data
    current_dataset = spark.read.format("csv").option("header", "true").option("delimiter", sep).load(new_file).withColumn('INCR_IND',lit('N')).dropDuplicates()
    current_dataset = current_dataset.select(column_with_ind)
    # create dataframe with deleted INCR_IND from previus data
    previous_dataset = spark.read.format("csv").option("header", "true").option("delimiter", sep).load(previous_file).withColumn('INCR_IND',lit('D')).dropDuplicates()
    previous_dataset = previous_dataset.select(column_with_ind)
    # union both dataframe
    df = current_dataset.union(previous_dataset)
    # partitioning based on column this is nothing but on what column you need to check updates
    my_window = Window.partitionBy(column).rowsBetween(-sys.maxsize, sys.maxsize)
    # will use previous partition and find duplicates so those records are unchanged
    df = df.withColumn('INCR_IND', when((count('*').over(my_window) > 1),'identical').otherwise(col('INCR_IND'))).dropDuplicates()
    # count of unchaged records
    unchanged = df.filter((col("INCR_IND") == "identical")).count()
    file.write("\nnumber_of_unchanged_count="+str(unchanged))

    df = df.filter((col("INCR_IND") != "identical"))
    # create another partition based on key only
    my_window = Window.partitionBy(key).rowsBetween(-sys.maxsize, sys.maxsize)
    # will use key partition and find duplicates and if INCR_IND is insert those records are updates
    df = df.withColumn('INCR_IND', when((count('*').over(my_window) > 1) & ((col("INCR_IND")=='N')),'U').otherwise(col('INCR_IND')))
    # will use key partition and find duplicates and if INCR_IND is deleted those records are old
    df = df.withColumn('INCR_IND', when((count('*').over(my_window) > 1) & ((col("INCR_IND")=='D')),'ignore').otherwise(col('INCR_IND')))
    # error out records which is not fulfill the match conditions
    error = df.filter(col(business_name).isNull())
    error_count = error.count()
    file.write("\nnumber_of_error_records="+str(error_count))
    # remove ignore and identical INCR_IND records from dataframe and also checking match attribute columns value is not null
    df = df.filter((col("INCR_IND") != "ignore") & (col("INCR_IND") != "identical") & ((col(business_name).isNotNull())))
    # get only New records
    insert_data = df.filter((col("INCR_IND") == "N"))
    # count of insert records
    insert = insert_data.count()
    # Get only Updated records
    update_data = df.filter((col("INCR_IND") == "U"))
    # count of update records
    update = update_data.count()
    # Get Deleted records
    deleted_data = df.filter((col("INCR_IND") == "D"))
    # count of deleted records
    deleted = deleted_data.count()
    # count of current dataset
    current_dataset_count = current_dataset.count()
    #count of previous dataset
    previous_dataset_count = previous_dataset.count()

    print("start time=",start_time)

    print("current_dataset_count=",current_dataset_count)
    file.write("\nnumber_of_current_dataset_count="+str(current_dataset_count))

    print("previous_dataset_count =",previous_dataset_count)
    file.write("\nnumber_of_previous_dataset_count="+str(previous_dataset_count))

    print("new=",insert)
    file.write("\nnumber_of_new_count="+str(insert))

    print("update=",update)
    file.write("\nnumber_of_update_count="+str(update))

    print("deleted=",deleted)
    file.write("\nnumber_of_deleted_count="+str(deleted))
    # as a testing total count should be addition of insert update and unchaged and then sustract deleted from that total count should match to cuurrent dataset count
    total_count = insert + update + unchanged + error_count

    print("Total count=",total_count)
    file.write("\nnumber_of_total_count="+str(total_count))
    file.write("\n")
    insert_data.coalesce(1).write.option("sep",sep).mode('overwrite').option("header","true").csv(app_home+"/output/insert/")
    update_data.coalesce(1).write.option("sep",sep).mode('overwrite').option("header","true").csv(app_home+"/output/update/")
    deleted_data.coalesce(1).write.option("sep",sep).mode('overwrite').option("header","true").csv(app_home+"/output/delete/")
    error.coalesce(1).write.option("sep",sep).mode('overwrite').option("header","true").csv(app_home+"/output/error/")
    print ("end time=",str(datetime.datetime.now()))

except Exception:
    traceback.print_exc(file=open(log_file,"a"))
    sys.exit(1)

sys.exit(0)
