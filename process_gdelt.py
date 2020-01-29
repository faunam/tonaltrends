from pyspark.sql.functions import col, udf, lit, explode
from pyspark.sql.types import ArrayType, IntegerType, StringType
from datetime import datetime

# in docs, write what each of these are for the diff sources
# should this be an enviro var? i need to reference it in mult files
columns = ["entity", "media", "date", "uniq_id", "tone", "mentions", "text"]
entities = ["facebook", "amazon", "jeff bezos",
            "donald trump", "bernie sanders"]  # figure out how to make this enviro


def get_titl_and_auth(meta_xml, page_source):
    # regex <PAGE_TITLE> __ </PAGE_TITLE>, <PAGE_AUTHORS> __ </PAGE_AUTHORS>,
    # returns a string
    #TODO: implement
    return "title"


def convert_tone(tone_list):
    # returns int
    return tone_list[0]


def convert_datetime(news_date):
    # returns whatever format datetime is in
    return datetime.strptime(str(news_date), '%Y%m%d%H%M%S')


def combine_mentions(people, orgs):
    # removes '[' and ']'
    translation = {91: None, 93: None}
    if people is None:  # more sustainable solution than this? fillna with ""?
        people_list = []
    else:
        people_list = people.translate(translation).split(";")
    if orgs is None:
        orgs_list = []
    else:
        orgs_list = orgs.translate(translation).split(";")

    mentions = people_list + orgs_list
    # might have to make string, idk tyoe restrictions on cols
    # return only relevant mentions
    return set(mentions).intersection(set(entities))


# i would make ent_list and argument but i dont think i can pass it through. global? #TODO
def set_entity(mentions):
    mention_entity_intersection = set(
        mentions).intersection(set(entities))
    if len(mention_entity_intersection) == 1:
        return mention_entity_intersection[0]
    else:
        return ""


def process_gdelt(gdelt_df):
    # mentions
    # could combine these udfs with above functions to make code cleaner
    udf_combine = udf(combine_mentions)
    gdelt_df = gdelt_df.withColumn(
        "mentions", udf_combine("people", "orgs"))
    #source (news)
    gdelt_df = gdelt_df.withColumn("media", lit(
        "news"))  # if this doesnt work try udf
    # date -> utc
    udf_datetime = udf(convert_datetime)
    gdelt_df = gdelt_df.withColumn("date", udf_datetime("date"))
    # tone -> just first num i think
    udf_tone = udf(convert_tone)
    gdelt_df = gdelt_df.withColumn("tone", udf_tone("tone"))
    # text -> title + author
    udf_meta = udf(get_titl_and_auth)
    gdelt_df = gdelt_df.withColumn("text", udf_meta("meta_xml", "source_page"))
    # entity
    gdelt_df = gdelt_df.withColumn("entity", explode(gdelt_df.mentions))
    # TODO: does this^ work
    # udf_entity = udf(set_entity)
    # gdelt_df = gdelt_df.withColumn("entity", udf_entity(
    #     "mentions"))

    return gdelt_df


def ingest_and_format(spark, s3_filepath):  # entities used to be a param
    gdelt_df = spark.read.option("sep", "\t").csv(
        s3_filepath)

    # rename gdelt columns
    old_col_nums = [0, 1, 3, 11, 13, 15, 26]
    new_col_names = ["uniq_id", "date", "source_page",
                     "people", "orgs", "tone", "meta_xml"]
    for index, col_num in enumerate(old_col_nums):
        gdelt_df = gdelt_df.withColumnRenamed(
            "_c" + str(col_num), new_col_names[index])

    gdelt_df.createOrReplaceTempView("all_news")

    gdelt_df = spark.sql("SELECT " + " ,".join(new_col_names) + " FROM all_news WHERE people RLIKE '" +
                         "|".join(entities) + "' OR orgs RLIKE '" + "|".join(entities) + "'")

    gdelt_df = process_gdelt(gdelt_df)

    return gdelt_df


def create_row_per_entity(gdelt_df):
    # create new row for each relevant entity mentioned in mentions
    # is there a way to do this en mass or do i need to do it one by one.
    # maybe udf with side effect of making more rows?
    # make intermediate dataframe with only the relevant colums and do from that.

    # just try this
    return gdelt_df.withColumn("entity", explode(gdelt_df.mentions))

    #########
    # gdelt_df.createOrReplaceTempView("formatted_news")
    # full_gdelt_df = spark.sql(
    #     "SELECT " + " ,".join(columns) + " FROM formatted_news WHERE NOT entity = ''")

    # # select records where entity is ""
    # incomplete_gdelt_df = spark.sql(
    #     "SELECT " + " ,".join(columns) + " FROM formatted_news WHERE entity = ''")
    # # duplicate them and write them to a new dataframe
