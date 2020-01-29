from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from datetime import datetime
import json

import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "mentions", "text"]


def format_tweet(tweet):
    # returns a dictionary of tweet features, formatted appropriately
    # tweet is a dictionary
    tweet["user"]["id_str"], tweet["user"]["screen_name"], tweet["id"], mentions, tweet["text"], date
    sentiment
    # mentions contains only  relevant mentions
    return {}


# change param to folder later and have it go through the file structure
def process_twitter_json(spark, df, ent_twitter_handles, filepath):

    # Do i need to take any measures to ensure the schema of gdelt and twitter dfs are the same? some way i dont have to hard code it?
    schema = StructType([StructField('source', StringType(), False), StructField('date', StringType(), False), StructField(
        'uniq_id', IntegerType(), True), StructField('mentions', StringType(), False), StructField('text', StringType(), False), StructField('tone', StringType(), False)])
    twitter_df = sqlContext.createDataFrame(spark.emptyRDD(), schema)

    # just run through this once, not once per entity.
    with open(filepath, "r") as tweet_file:
        for tweet_json in tweet_file:

            tweet = json.loads(tweet_json)
            mentions = [mention["screen_name"]
                        for mention in tweet["user_mentions"]]
            mention_entity_intersection = set(
                mentions).intersection(set(ent_twitter_handles))

            if len(mention_entity_intersection) > 0:
                feature_dict = format_tweet(tweet)
                # ensures that order of features is correct and makes adding new columns easier.
                feature_list = [feature_dict[column] for column in columns[1:]]
                new_rows_list = []
                for entity in mention_entity_intersection:
                    # creates new records for each relevant entity mentioned
                    # add entity not ent twitter handle
                    new_rows_list.append(tuple([entity] + feature_list))
                tweet_records = spark.createDataFrame(new_rows_list)
                df = df.union(tweet_records)
