import pyspark.sql.types as t
from app.Columns import Columns
from app.Tables import Tables


def create_schema(table_name: Tables):
    if table_name == Tables.TITLE_RATINGS:
        return t.StructType(
            [
                t.StructField(Columns.TCONST.value, t.StringType()),
                t.StructField(Columns.AVERAGE_RATING.value, t.FloatType()),
                t.StructField(Columns.NUM_VOTES.value, t.IntegerType()),
            ]
        )
    elif table_name == Tables.TITLE_EPISODE:
        return t.StructType(
            [
                t.StructField(Columns.TCONST.value, t.StringType()),
                t.StructField(Columns.PARENT_TCONST.value, t.StringType()),
                t.StructField(Columns.SEASON_NUMBER.value, t.IntegerType()),
                t.StructField(Columns.EPISODE_NUMBER.value, t.IntegerType()),
            ]
        )
    elif table_name == Tables.TITLE_CREW:
        return t.StructType(
            [
                t.StructField(Columns.TCONST.value, t.StringType()),
                t.StructField(Columns.DIRECTORS.value, t.StringType()),
                t.StructField(Columns.WRITERS.value, t.StringType()),
            ]
        )
    elif table_name == Tables.TITLE_BASICS:
        return t.StructType(
            [
                t.StructField(Columns.TCONST.value, t.StringType()),
                t.StructField(Columns.TITLE_TYPE.value, t.StringType()),
                t.StructField(Columns.PRIMARY_TITLE.value, t.StringType()),
                t.StructField(Columns.ORIGINAL_TITLE.value, t.StringType()),
                t.StructField(Columns.IS_ADULT.value, t.IntegerType()),
                t.StructField(Columns.START_YEAR.value, t.StringType()),
                t.StructField(Columns.END_YEAR.value, t.StringType()),
                t.StructField(Columns.RUNTIME_MINUTES.value, t.IntegerType()),
                t.StructField(Columns.GENRES.value, t.StringType()),
            ]
        )
    elif table_name == Tables.NAME_BASICS:
        return t.StructType(
            [
                t.StructField(Columns.NCONST.value, t.StringType()),
                t.StructField(Columns.PRIMARY_NAME.value, t.StringType()),
                t.StructField(Columns.BIRTH_YEAR.value, t.StringType()),
                t.StructField(Columns.DEATH_YEAR.value, t.StringType()),
                t.StructField(Columns.PRIMARY_PROFESSION.value, t.StringType()),
                t.StructField(Columns.KNOWN_FOR_TITLES.value, t.StringType()),
            ]
        )
    elif table_name == Tables.TITLE_AKAS:
        return t.StructType(
            [
                t.StructField(Columns.TITLE_ID.value, t.StringType()),
                t.StructField(Columns.ORDERING.value, t.IntegerType()),
                t.StructField(Columns.TITLE.value, t.StringType()),
                t.StructField(Columns.REGION.value, t.StringType()),
                t.StructField(Columns.LANGUAGE.value, t.StringType()),
                t.StructField(Columns.TYPES.value, t.StringType()),
                t.StructField(Columns.ATTRIBUTES.value, t.StringType()),
                t.StructField(Columns.IS_ORIGINAL_TITLE.value, t.IntegerType()),
            ]
        )
