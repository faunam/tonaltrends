from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
from datetime import datetime
import json
from datetime import datetime

import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "mentions", "text"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "sensanders"]  # TODO: better soln than this
twit_to_ent = {"facebook": "facebook", "amazon": "amazon", "jeffbezos": "jeff bezos",
               "realdonaldtrump": "donald trump", "sensanders": "bernie sanders"}


def format_tweet(tweet, mentions):
    # returns a dictionary of tweet features, formatted appropriately
    # tweet is a dictionary
    date = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S %z %Y')

    format_dict = {
        "media": "twitter",
        "date": date,
        "uniq_id": tweet["id"],
        "tone": 0,  # TODO: implement sent analy
        "mentions": mentions,
        "text": tweet["text"]
    }
    # mentions contains only  relevant mentions
    return format_dict


# change filepath param to folder later and have it go through the file structure
def ingest_and_format(spark, filepath):  # ent_twitter_handles was a param
    # i think there might be a better way to do this than record by record?

    # Do i need to take any measures to ensure the schema of gdelt and twitter dfs are the same? some way i dont have to hard code it?
    # schema = StructType([StructField('entity', StringType(), False), StructField('media', StringType(), False), StructField('date', StringType(), False), StructField(
    #     'uniq_id', IntegerType(), True), StructField('tone', IntegerType(), False), StructField('mentions', StringType(), False), StructField('text', StringType(), False)])
    # twitter_df = sqlContext.createDataFrame(spark.emptyRDD(), schema)

    # just run through this once, not once per entity.
    with open(filepath, "r") as tweet_file:
        df_rows = []
        for tweet_json in tweet_file:

            tweet = json.loads(tweet_json)
            # do regex on text to find mentions (of ent name, not handle)
            mentions = [mention["screen_name"]
                        for mention in tweet["user_mentions"]]
            mention_ent_handle_intersection = set(
                mentions).intersection(set(ent_twitter_handles))
            relevant_mentions = [twit_to_ent[handle]
                                 for handle in mention_ent_handle_intersection]

            if len(relevant_mentions) > 0:
                feature_dict = format_tweet(tweet, relevant_mentions)
                # ensures that order of features is correct and makes adding new columns easier.
                feature_list = [feature_dict[column] for column in columns[1:]]
                for entity in relevant_mentions:
                    # creates new records for each relevant entity mentioned
                    df_rows.append(
                        tuple([entity] + feature_list))

        return spark.createDataFrame(df_rows, columns)
