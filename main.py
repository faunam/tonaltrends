from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import process_twitter
import process_gdelt

# make these enviro vars
db_url = "jdbc:postgresql://tone-db.ccg3nx1k7it5.us-west-2.rds.amazonaws.com:5432/postgres"
db_properties = {"user": "faunam", "password": "",
                 "driver": "org.postgresql.Driver"}

entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "sensanders"]  # make dictionary instead
ent_to_twit = {}
twit_to_ent = {}
# maybe a class to hold relevant entities, and column names

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GDELTToneTrends")\
        .getOrCreate()

# create twitter dataframe correctly formatted
    twitter_df = process_twitter.format(
        spark, "2015-03-29-06-33.json")
    # maybe a class for passing around all these args?
    twitter_df.printSchema()
    twitter_df.select('*').show(10)

# create gdelt df correctly formatted
# https://s3.console.aws.amazon.com/s3/buckets/gdelt-open-data/v2/gkg/?region=us-east-1
# GDELT ENDS ON APRIL 16 2019!!!! UH OH!!!! but i got new data??? waht???
    # gdelt_df = process_gdelt.format(
    #     spark, "s3a://gdelt-sample/20200116003000.gkg.csv")  # "s3a://gdelt-open-data/v2/gkg/*.gkg.csv"
    gdelt_df = process_gdelt.format(
        spark, "s3a://gdelt-open-data/v2/gkg/2015032906*.gkg.csv")
#    gdelt_df = gdelt_df.union(process_gdelt.format(
#        spark, "s3a://gdelt-open-data/v2/gkg/2015022010*.gkg.csv"))

    # example key: v2/gkg/20190415124500.gkg.csv. these files start at v2/gkg/20150218230000.gkg.cs
    # so i can stop my twitter from before that point too.

    gdelt_df.printSchema()
    gdelt_df.select('*').show(10)

    # write to db
    # do i join them first and then write to db or just write both to db?
    gdelt_df.write.jdbc(url=db_url, table="full_sample",
                        mode="overwrite", properties=db_properties)
    twitter_df.write.jdbc(url=db_url, table="full_sample",
                          mode="append", properties=db_properties)

    spark.stop()
