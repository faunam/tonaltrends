from datetime import datetime
import json
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
import re

#import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "mentions", "text"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "sensanders"]  # TODO: better soln than this
twit_to_ent = {"facebook": "facebook", "amazon": "amazon", "jeffbezos": "jeff bezos",
               "realdonaldtrump": "donald trump", "sensanders": "bernie sanders"}
entities_extended = {"facebook": ["facebook"], "amazon": ["amazon"], "jeff bezos": ["jeff bezos", "bezos"],
                     "donald trump": ["donald trump", "trump"], "bernie sanders": ["bernie sanders", "bernie", "sanders"]}
# TODO: automate this list creation ^
entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]


def format_tweet(tweet, mentions):
    # returns a dictionary of tweet features, formatted appropriately
    # tweet is a dictionary
    date = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S %z %Y')

    format_dict = {
        "media": "twitter",
        "date": date,  # TODO: fix
        "uniq_id": tweet["id"],
        "tone": 0,  # TODO: implement sent analy
        "mentions": mentions,
        "text": tweet["text"]
    }
    # mentions contains only  relevant mentions
    return format_dict


def find_mentions(tweet):
    # returns set (list) of relevant mentions in @s, hashtags, and text
    mentions = [mention["screen_name"]
                for mention in tweet["entities"]["user_mentions"]]
    mention_ent_handle_intersection = set(
        mentions).intersection(set(ent_twitter_handles))
    relevant_mentions = {twit_to_ent[handle]
                         for handle in mention_ent_handle_intersection}

    # do regex on text and hashtags to find mentions (of ent name, not handle)
    for entity in entities:
        flattened_refs = [ref.replace(
            " ", "") for ref in entities_extended[entity]]  # for hashtags
        possible_ent_refs = list(
            set(entities_extended[entity] + flattened_refs))  # remove duplicates #TODO: extract this logic so you're not doing it every time.
        # TODO: don't match 'sanderson' for instance - but could be at end/beginning of string, or a space.
        regex_str = "(?:" + "|".join(possible_ent_refs) + ")"
        pattern = re.compile(regex_str)
        hashtags = " ".join([tag_obj["text"]
                             for tag_obj in tweet["entities"]["hashtags"]])

        in_text_mention = re.search(
            pattern, tweet["text"] + " " + hashtags, re.I).group()
        if in_text_mention is not None:
            relevant_mentions.add(entity)

    return list(relevant_mentions)

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

            try:
                relevant_mentions = find_mentions(tweet)

                if len(relevant_mentions) > 0:
                    feature_dict = format_tweet(tweet, relevant_mentions)
                    # ensures that order of features is correct and makes adding new columns easier.
                    feature_list = [feature_dict[column]
                                    for column in columns[1:]]
                    for entity in relevant_mentions:
                        # creates new records for each relevant entity mentioned
                        df_rows.append(
                            tuple([entity] + feature_list))
            except KeyError:
                continue

        return spark.createDataFrame(df_rows, columns)
