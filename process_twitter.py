from dateutil import parser
import json
from pyspark.sql.functions import col, udf, lit, explode
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType, TimestampType
from pyspark.sql import DataFrame
import re
from unidecode import unidecode
from functools import reduce

import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "text"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "sensanders", "berniesanders"]  # TODO: better soln than this
twit_to_ent = {"facebook": "facebook", "amazon": "amazon", "jeffbezos": "jeff bezos",
               "realdonaldtrump": "donald trump", "sensanders": "bernie sanders", "berniesanders": "bernie sanders"}
ent_to_twit = {"facebook": ["facebook"], "amazon": ["amazon"], "jeff bezos": ["jeffbezos"],
               "donald trump": ["realdonaldtrump"], "bernie sanders": ["berniesanders", "sensanders"]}
entities_extended = {"facebook": ["facebook"], "amazon": ["amazon"], "jeff bezos": ["jeff bezos", "bezos"],
                     "donald trump": ["donald trump", "trump"], "bernie sanders": ["bernie sanders", "bernie", "sanders"]}
# TODO: automate this list creation ^
entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]


def format_tweet(tweet, mentions):
    # returns a dictionary of tweet features, formatted appropriately
    # tweet is a dictionary
    date = parser.parse(tweet["created_at"])  # .strftime("%m/%d/%Y, %H:%M:%S")

    format_dict = {
        "media": "twitter",
        "date": date,  # TODO: fix
        "uniq_id": tweet["id"],
        "tone": tweet_sentiment.get_tweet_tone(tweet),
        "mentions": mentions,
        "text": tweet["text"]
    }
    # mentions contains only  relevant mentions
    return format_dict


def search_at_mentions(tweet, possible_refs):
    mentions = [mention["screen_name"]
                for mention in tweet["entities"]["user_mentions"]]
    mention_ent_handle_intersection = set(
        mentions).intersection(set(possible_refs))
    return [twit_to_ent[handle]
            for handle in mention_ent_handle_intersection]


def search_text(tweet, possible_refs):
    # requires space before and after match
    text_regex_str = " (?:" + "|".join(possible_refs) + ") "
    # remove duplicates #TODO: extract this logic so you're not doing it every time.
    text_pattern = re.compile(text_regex_str, re.I)

    return re.search(text_pattern, " " + tweet["text"] + " ")


def search_tags(tweet, possible_refs):
    hashtags = " ".join([unidecode(tag_obj["text"])
                         for tag_obj in tweet["entities"]["hashtags"]])
    flattened_refs = [ref.replace(" ", "")
                      for ref in possible_refs]  # for hashtags
    tag_regex_str = "(?:" + "|".join(flattened_refs) + ")"
    # remove duplicates #TODO: extract this logic so you're not doing it every time.
    tag_pattern = re.compile(tag_regex_str, re.I)

    return re.search(tag_pattern, hashtags)


def find_mentions(tweet):
    # returns set (list) of relevant mentions in @s, hashtags, and text
    # mentions in text must be surounded by spaces, but in hashtags this isn't necessary.
    relevant_mentions = set(search_at_mentions(tweet, ent_twitter_handles))

    for entity in entities:

        possible_ent_refs = entities_extended[entity]

        text_search_result = search_text(tweet, possible_ent_refs)
        tag_search_result = search_tags(tweet, possible_ent_refs)
        if text_search_result is not None or tag_search_result is not None:
            relevant_mentions.add(entity)

    return list(relevant_mentions)

# change filepath param to folder later and have it go through the file structure


def format(spark, filepath):  # ent_twitter_handles was a param
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

            # would combine with below but i dont want to evaluate relevant mentions before checking this
            try:
                if tweet["lang"] != "en":
                    continue
                try:
                    tweet["text"] = unidecode(
                        tweet["retweeted_status"]["extended_tweet"]["full_text"])
                except KeyError:
                    tweet["text"] = unidecode(tweet["text"])

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


def get_hashtag_text(hashtags):
    if hashtags is None:
        return ""
    return " ".join([hashtag["text"] for hashtag in hashtags])


def get_mentions_text(mentions):
    if mentions is None:
        return ""
    return " ".join([mention["screen_name"] for mention in mentions])


def get_mentions_and_ent(df, spark):
    # prepare relevant columns
    # tags
    udf_hashtag_text = udf(get_hashtag_text)
    df = df.withColumn("hashtag_text", udf_hashtag_text(
        "entities.hashtags"))
    # @s
    udf_mentions_text = udf(get_mentions_text)
    df = df.withColumn("mention_text", udf_mentions_text(
        "entities.user_mentions"))
    df.createOrReplaceTempView("tweets_modded")

    mentioned_dfs = []
    for entity in entities:
        possible_tag_refs = [ref.replace(" ", "")
                             for ref in entities_extended[entity]]
        possible_text_refs = entities_extended[entity]
        possible_mention_refs = ent_to_twit[entity]
        # select where any possible ref is in tags or mentions or text
        # may need to create temporary view within this function ("all_tweets")
        mentioned_df = spark.sql("SELECT * FROM tweets_modded WHERE hashtag_text RLIKE '"
                                 + "|".join(possible_tag_refs) +
                                 "' OR mention_text RLIKE '"
                                 + "|".join(possible_mention_refs) +
                                 "' OR text RLIKE '"
                                 + "|".join(possible_text_refs) + "'")
        mentioned_df = mentioned_df.withColumn("entity", lit(entity))
        mentioned_dfs.append(mentioned_df)

    return reduce(DataFrame.unionAll, mentioned_dfs)


def get_tone(text):
    return tweet_sentiment.get_tweet_tone(text)


def get_text(text, retweeted):
    try:
        return unidecode(retweeted["extended_tweet"]["full_text"])
    except:
        return unidecode(text)
    # if fulltext is None:
    #     return text
    # return fulltext["full_text"]


def convert_datetime(date):
    # returns whatever format datetime is in
    return parser.parse(str(date))


def process_twitter(twitter_df, spark):
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
    twitter_df.select("tone").show(2)
    #mentions, entity
    twitter_df = get_mentions_and_ent(twitter_df, spark)

# TODO: filter by entity before doing other transformatinos so it's less computationally intense
    return twitter_df.select(*columns)


def format2(spark, s3_filepath):  # entities used to be a param
    twitter_df = spark.read.json(
        s3_filepath)  # might want to think about partitioning
    # https://stackoverflow.com/questions/37445054/spark-difference-when-read-in-gz-and-bz2

    # could filter for mentions here maybe? not sure how to handle extended tweets conditional
    twitter_df.createOrReplaceTempView("all_tweets")

    twitter_df = spark.sql("SELECT * FROM all_tweets WHERE lang = 'en'")

    twitter_df = process_twitter(twitter_df, spark)

    return twitter_df
