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
    twitter_df = process_twitter.ingest_and_format(
        spark, "../sample_twitter_s3/29.json")
    # maybe a class for passing around all these args?
    twitter_df.printSchema()
    twitter_df.select('*').show(10)

# create gdelt df correctly formatted
    gdelt_df = process_gdelt.ingest_and_format(
        spark, "s3a://gdelt-sample/20200116003000.gkg.csv")

    gdelt_df.printSchema()
    gdelt_df.select('*').show(10)

    # write to db
    # do i join them first and then write to db or just write both to db?
    gdelt_df.write.jdbc(url=db_url, table="full_sample",
                        mode="overwrite", properties=db_properties)
    twitter_df.write.jdbc(url=db_url, table="full_sample",
                          mode="append", properties=db_properties)

    spark.stop()
