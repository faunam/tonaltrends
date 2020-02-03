from dateutil import parser
import json
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
import re
from unidecode import unidecode

import tweet_sentiment

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "mentions", "text"]
ent_twitter_handles = ["facebook", "amazon", "jeffbezos",
                       "realdonaldtrump", "sensanders", "berniesanders"]  # TODO: better soln than this
twit_to_ent = {"facebook": "facebook", "amazon": "amazon", "jeffbezos": "jeff bezos",
               "realdonaldtrump": "donald trump", "sensanders": "bernie sanders", "berniesanders": "bernie sanders"}
entities_extended = {"facebook": ["facebook"], "amazon": ["amazon"], "jeff bezos": ["jeff bezos", "bezos"],
                     "donald trump": ["donald trump", "trump"], "bernie sanders": ["bernie sanders", "bernie", "sanders"]}
# TODO: automate this list creation ^
entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]


def format_tweet(tweet, mentions):
    # returns a dictionary of tweet features, formatted appropriately
    # tweet is a dictionary
    date = parser.parse(tweet["created_at"])

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
                        tweet[["retweeted_status"]"extended_tweet"]["full_text"])
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
