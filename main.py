from pyspark import SparkConf
from pyspark.sql import SparkSession
from app import base
from app import read_write as rw
from app import create_schema as create_schema
from app.Tables import Tables
from app.Columns import Columns
from pyspark.sql.functions import isnull, col, isnan, when, sum, broadcast, count, avg, coalesce, round
from pyspark.sql.window import Window

spark_session = (
    SparkSession.builder.master("local")
    .appName("imdb")
    .config(conf=SparkConf())
    .getOrCreate()
)

# TITLE_RATINGS
print("-------------TITLE_RATINGS-------------")
title_ratings_schema = create_schema.create_schema(Tables.TITLE_RATINGS)
title_ratings = rw.read("data/data.tsv", title_ratings_schema, spark_session)
title_ratings.show()
title_ratings.printSchema()
title_ratings.select(
    Columns.AVERAGE_RATING.value, Columns.NUM_VOTES.value
).describe().show()
base.display_null_values_percentage_table(title_ratings)
# transformations
print("Count with duplicates - ", title_ratings.count())
title_ratings = title_ratings.dropDuplicates()
print("Count without duplicates - ", title_ratings.count())
rw.write(title_ratings, "data/title_ratings")

# TITLE_EPISODE
print("-------------TITLE_EPISODE-------------")
title_episode_schema = create_schema.create_schema(Tables.TITLE_EPISODE)
title_episode = rw.read("data/data-2.tsv", title_episode_schema, spark_session)
title_episode.show()
title_episode.printSchema()
title_episode.select(Columns.SEASON_NUMBER.value, Columns.EPISODE_NUMBER.value).describe().show()
base.display_null_values_percentage_table(title_episode)
# transformations
# drop duplicates
print("Count with duplicates - ", title_episode.count())
title_episode = title_episode.dropDuplicates()
print("Count without duplicates - ", title_episode.count())
# consider 0 as null for EPISODE_NUMBER column
title_episode = base.zero_to_none(title_episode, Columns.EPISODE_NUMBER)
base.display_null_values_percentage_table(title_episode)
title_episode.select(Columns.SEASON_NUMBER.value, Columns.EPISODE_NUMBER.value).describe().show()
rw.write(title_episode, "data/title_episode")

# TITLE_CREW
print("-------------TITLE_CREW-------------")
title_crew_schema = create_schema.create_schema(Tables.TITLE_CREW)
title_crew = rw.read("data/data-3.tsv", title_crew_schema, spark_session)
title_crew.show()
title_crew.printSchema()
base.display_null_values_percentage_table(title_crew)
# transformations
# drop duplicates
print("Count with duplicates - ", title_crew.count())
title_crew = title_crew.dropDuplicates()
print("Count without duplicates - ", title_crew.count())
# # handle columns that contain list
title_crew = base.split_list(title_crew, Columns.DIRECTORS)
title_crew = base.split_list(title_crew, Columns.WRITERS)
title_crew.orderBy(Columns.TCONST.value).show()
base.display_null_values_percentage_table(title_crew)
rw.write(title_crew, "data/title_crew")

# TITLE_BASICS
print("-------------TITLE_BASICS-------------")
title_basics_schema = create_schema.create_schema(Tables.TITLE_BASICS)
title_basics = rw.read("data/data-4.tsv", title_basics_schema, spark_session)
title_basics.show()
title_basics.printSchema()
title_basics.select(
    Columns.START_YEAR.value, Columns.END_YEAR.value, Columns.RUNTIME_MINUTES.value
).describe().show()
base.display_null_values_percentage_table(title_basics)
print("Unique values for TITLE_TYPE:")
base.get_unique_values_for_categorical_columns(title_basics, Columns.TITLE_TYPE).show()
print("Unique values for IS_ADULT:")
base.get_unique_values_for_categorical_columns(title_basics, Columns.IS_ADULT).show()
# transformations
# drop duplicates
print("Count with duplicates - ", title_basics.count())
title_basics = title_basics.dropDuplicates()
print("Count without duplicates - ", title_basics.count())
# drop columns with more than 90% of null
title_basics = title_basics.drop(Columns.END_YEAR.value)
# convert string to date
title_basics = base.convert_string_to_date(title_basics, Columns.START_YEAR, "yyyy")
title_basics.printSchema()
# consider 0 as null for RUNTIME_MINUTES column
title_basics = base.zero_to_none(title_basics, Columns.RUNTIME_MINUTES)
base.display_null_values_percentage_table(title_basics)
title_basics.select(
    Columns.START_YEAR.value, Columns.RUNTIME_MINUTES.value
).describe().show()
rw.write(title_basics, "data/title_basics")


# NAME_BASICS
print("-------------NAME_BASICS-------------")
name_basics_schema = create_schema.create_schema(Tables.NAME_BASICS)
name_basics = rw.read("data/data-5.tsv", name_basics_schema, spark_session)
name_basics.show()
name_basics.printSchema()
base.display_null_values_percentage_table(name_basics)
# transformations
print("Count with duplicates - ", name_basics.count())
name_basics = name_basics.dropDuplicates()
print("Count without duplicates - ", name_basics.count())
# handle columns that contain list
name_basics = base.split_list(name_basics, Columns.KNOWN_FOR_TITLES)
name_basics.show()
rw.write(name_basics, "data/name_basics")

# TITLE_AKAS
print("-------------TITLE_AKAS-------------")
title_akas_schema = create_schema.create_schema(Tables.TITLE_AKAS)
title_akas = rw.read("data/data-6.tsv", title_akas_schema, spark_session)
title_akas.show()
title_akas.printSchema()
title_akas.select(
    Columns.ORDERING.value, Columns.IS_ORIGINAL_TITLE.value
).describe().show()
base.display_null_values_percentage_table(title_akas)
print("Unique values for REGION:")
base.get_unique_values_for_categorical_columns(title_akas, Columns.REGION).show(100)
print("Unique values for TYPES:")
base.get_unique_values_for_categorical_columns(title_akas, Columns.TYPES).show(100)
print("Unique values for LANGUAGE:")
base.get_unique_values_for_categorical_columns(title_akas, Columns.LANGUAGE).show(100)
# transformations
# drop columns with more than 90% of null
title_akas = title_akas.drop(Columns.ATTRIBUTES.value)
# drop duplicates
print("Count with duplicates - ", title_akas.count())
title_akas = title_akas.dropDuplicates()
print("Count without duplicates - ", title_akas.count())
rw.write(title_akas, "data/title_akas")

# Who are directors of titles with the highest rate (top 10)?

title_ratings.orderBy(col(Columns.AVERAGE_RATING.value).desc()).limit(10).join(
    title_crew, on=Columns.TCONST.value, how="left"
).join(
    title_basics, on=Columns.TCONST.value, how="left"
).join(
    name_basics, title_crew[Columns.DIRECTORS.value] == name_basics[Columns.NCONST.value], how="left",
).select(
    name_basics[Columns.PRIMARY_NAME.value].alias("director_name"),
    title_basics[Columns.PRIMARY_TITLE.value],
    title_basics[Columns.ORIGINAL_TITLE.value],
    title_basics[Columns.TITLE_TYPE.value],
    title_ratings[Columns.AVERAGE_RATING.value],
).distinct().show()

# How many titles of each title type are there?

title_basics.join(title_ratings, on=Columns.TCONST.value, how="right"
).groupBy(
    Columns.TITLE_TYPE.value
).agg({'*': "count"}).show()

# What titles have the highest rate in Ukraine (top 5)?

title_ratings.join(title_basics.alias('title_basics'), on=Columns.TCONST.value, how="left"
).join(title_akas.alias('title_akas'), title_ratings.tconst == title_akas.titleId, 'left'
).filter(col(Columns.REGION.value) == "UA"
).orderBy(col(Columns.AVERAGE_RATING.value).desc()).limit(5
).select(
    title_akas[Columns.REGION.value],
    title_basics[Columns.PRIMARY_TITLE.value],
    title_basics[Columns.ORIGINAL_TITLE.value],
    title_basics[Columns.TITLE_TYPE.value],
    title_ratings[Columns.AVERAGE_RATING.value]
).distinct().show()

# What is the average runtime minute for every title type?

window = Window.partitionBy(Columns.TITLE_TYPE.value)
title_basics.filter(col(Columns.RUNTIME_MINUTES.value).isNotNull()
).withColumn(
    "avg_runtime_minutes_by_type", avg(col(Columns.RUNTIME_MINUTES.value)).over(window)
).select(
     Columns.TITLE_TYPE.value,
     round("avg_runtime_minutes_by_type", 1).alias("avg_runtime_minutes_by_type")
).distinct().show()

# Do we have more titles for children or for adults?

window_is_adult = Window.partitionBy(Columns.IS_ADULT.value)
title_basics.filter(col(Columns.TCONST.value).isNotNull() & col(Columns.IS_ADULT.value).isin(0, 1)
).withColumn(
    "count", count(col(Columns.TCONST.value)).over(window_is_adult)
).select(
     Columns.IS_ADULT.value,
     "count"
).distinct().show()

# How many titles do we have per each region (top 20)?

title_akas.filter(col(Columns.REGION.value).isNotNull()
).groupBy(
    Columns.REGION.value
).agg(
    {'*': "count"}
).orderBy(
    col("count(1)").desc()
).show()



# What are the other titles the director of movie with the highest number of votes is known for?

# max_votes_tconst = title_ratings.orderBy(col(Columns.NUM_VOTES.value).desc()).first()[Columns.TCONST.value]
# title_ratings.join(
#     title_crew, on=Columns.TCONST.value, how="left"
# ).join(
#     name_basics, name_basics.nconst == title_crew.directors, "left"
# ).join(
#     title_basics, name_basics.knownForTitles == title_basics.tconst, "left"
# ).filter(
#     (title_basics.tconst == max_votes_tconst) 
# ).select(
#     name_basics[Columns.PRIMARY_NAME.value].alias("director_name"),
#     title_basics[Columns.TCONST.value],
#     title_basics[Columns.PRIMARY_TITLE.value],
#     title_basics[Columns.ORIGINAL_TITLE.value],
#     title_basics[Columns.TITLE_TYPE.value],
#     title_ratings[Columns.AVERAGE_RATING.value],
# ).distinct().show()

