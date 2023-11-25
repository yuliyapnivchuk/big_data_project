from pyspark.sql import DataFrame
from pyspark.sql.functions import isnull, col, isnan, when, sum, round, to_date, explode_outer, split, coalesce
from app.Columns import Columns


def calculate_null_values_percentage(df: DataFrame):
    return df.select(
        [
            (round(sum(when(isnull(c), 1).otherwise(0)) / df.count() * 100, 2)).alias(c)
            for c in df.columns
        ]
    )


def display_null_values_percentage_table(df: DataFrame):
    print("--------------------------------------------------")
    print("Null values % :\n")
    null_df = calculate_null_values_percentage(df)
    null_df.show()


def convert_string_to_date(df: DataFrame, column: Columns, date_format: str):
    return df.withColumn(
        column.value,
        when(col(column.value).isin(0, None), None).otherwise(
            to_date(column.value, date_format)
        ),
    )


def zero_to_none(df: DataFrame, column: Columns):
    return df.withColumn(
        column.value,
        when(col(column.value).isin(0, None), None).otherwise(df[column.value]),
    )


def get_unique_values_for_categorical_columns(df: DataFrame, column: Columns):
    return df.groupBy(column.value).agg({'*': "count"})


def split_list(df: DataFrame, column: Columns):
    list_col_name = column.value + "_list"
    split_df = df.withColumn(list_col_name, split(col(column.value), ","))
    exploded_df = split_df.withColumn(column.value, explode_outer(col(list_col_name))).drop(list_col_name)
    return exploded_df
