from dateutil import parser
import json
from pyspark.sql.functions import col, udf, lit, explode
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType, TimestampType
from pyspark.sql import SQLContext,  DataFrame
import re
from unidecode import unidecode
from functools import reduce

import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "text"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "berniesanders"]  # TODO: better soln than this
twit_to_ent = {"facebook": "facebook", "amazon": "amazon", "jeffbezos": "jeff bezos",
               "realdonaldtrump": "donald trump", "berniesanders": "bernie sanders"}
ent_to_twit = {"facebook": "facebook", "amazon": "amazon", "jeff bezos": "jeffbezos",
               "donald trump": "realdonaldtrump", "bernie sanders": "berniesanders"}
entities_extended = {"facebook": ["facebook"], "amazon": ["amazon"], "jeff bezos": ["jeff bezos", "bezos"],
                     "donald trump": ["donald trump", "trump"], "bernie sanders": ["bernie sanders", "bernie", "sanders"]}
# TODO: automate this list creation ^
entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]


def get_tone(text):
    return tweet_sentiment.get_tweet_tone(text)


def get_text(text, retweeted):
    try:
        return unidecode(retweeted["extended_tweet"]["full_text"])
    except:
        return unidecode(text)


def convert_datetime(date):
    # returns whatever format datetime is in
    return parser.parse(str(date))


def process_twitter(twitter_df):
    # could combine these udfs with above functions to make code cleaner
    # uniq_id
    twitter_df = twitter_df.withColumnRenamed("id", "uniq_id")
    # source (news)
    twitter_df = twitter_df.withColumn("media", lit(
        "twitter"))  # if this doesnt work try udf
    # date -> utc
    udf_datetime = udf(convert_datetime, TimestampType())  # StringType()
    twitter_df = twitter_df.withColumn("date", udf_datetime("created_at"))
    # text
    udf_text = udf(get_text)
    twitter_df = twitter_df.withColumn("text", udf_text(
        "text", "retweeted_status"))
    # tone
    udf_tone = udf(get_tone, IntegerType())
    twitter_df = twitter_df.withColumn("tone", udf_tone("text"))
    # entity
    twitter_df = twitter_df.withColumn("entity", explode(twitter_df.mentions))
    return twitter_df.select(*columns)


def relevant_at_mentions(ats, possible_refs):
    ats = [mention["screen_name"]
           for mention in ats] if ats else []
    ats_ent_handle_intersection = set(
        ats).intersection(set(possible_refs))
    return {twit_to_ent[handle]
            for handle in ats_ent_handle_intersection}


def text_search_result(text, possible_refs):
    # requires space before and after match
    text_regex_str = " (?:" + "|".join(possible_refs) + ") "
    text_pattern = re.compile(text_regex_str, re.I)
    # TODO: extract this logic so you're not doing it every time.

    return re.search(text_pattern, " " + text + " ")


def tag_search_result(hashtags_text, possible_refs):
    flattened_refs = [ref.replace(" ", "")
                      for ref in possible_refs]  # for hashtags
    tag_regex_str = "(?:" + "|".join(flattened_refs) + ")"
    tag_pattern = re.compile(tag_regex_str, re.I)
    # TODO: extract this^ logic so you're not doing it every time.
    return re.search(tag_pattern, hashtags_text)


def get_mentions(hashtags, ats, text):
    # for each entity, check if in text ats or hashtags, add to list,
    # return list

    # im not going to bother searching extended text for now
    text = unidecode(text)
    hashtags_text = " ".join([hashtag["text"]
                              for hashtag in hashtags]) if hashtags else ""

    entities_mentioned = relevant_at_mentions(
        ats, [ent_to_twit[entity] for entity in entities])
    for entity in entities:
        possible_tag_refs = [ref.replace(" ", "")
                             for ref in entities_extended[entity]]
        possible_text_refs = entities_extended[entity]
        # regex -> look back at json parsing version of this
        if text_search_result(text, possible_text_refs) or tag_search_result(hashtags_text, possible_tag_refs):
            entities_mentioned.add(entity)

    mentions = list(entities_mentioned)

    if not mentions:
        return None  # is this how to get it to be null in the df?
    return ",".join(entities_mentioned)  # list(entities_mentioned)


def format_df(spark, s3_filepath):  # entities used to be a param
    twitter_df = spark.read.json(
        s3_filepath)  # might want to think about partitioning
    # https://stackoverflow.com/questions/37445054/spark-difference-when-read-in-gz-and-bz2

    # could filter for mentions here maybe? not sure how to handle extended tweets conditional
    twitter_df.createOrReplaceTempView("all_tweets")

    #spark.udf.register("mentionsEntity", get_mentions, ArrayType())
    spark.udf.register(
        "mentionsEntity", lambda x, y, z: get_mentions(x, y, z), ArrayType(StringType()))  # should be able to specify ArrayType...
    twitter_df = spark.sql("""SELECT first_value(id) as id, first_value(created_at) as created_at, first_value(retweeted_status) as retweeted_status, first_value(text) as text, mentionsEntity('entities.hashtags', 'entities.user_mentions', 'text') as mentions FROM all_tweets WHERE lang = 'en' HAVING mentionsEntity('entities.hashtags', 'entities.user_mentions', 'text') IS NOT NULL""")
    # also only select relevant columns
    # TODO create a query selecting for entity mentions???
    # is it possible to create a mentions column and check if there are mentions at the same time (filtration and column creation?)
    # ill do separate at first
    # TODO also fucking..... distribute to workers plz for the love of god and it wont take so long

    twitter_df = process_twitter(twitter_df)

    return twitter_df
