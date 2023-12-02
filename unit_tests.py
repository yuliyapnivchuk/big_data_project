import unittest
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkConf
from app import base
import pyspark.sql.types as t
from app.Columns import Columns
from pyspark.sql.functions import col


class TestBaseMethods(unittest.TestCase):
    
    def get_spark_session(self):
        return (
            SparkSession.builder.master("local")
            .appName("imdb")
            .config(conf=SparkConf())
            .getOrCreate()
        )

    def create_test_data_frame(self):
        data = [
            ("A", 1, 100, "1996"),
            ("A", 2, None, "2001"),
            ("A", 0, 300, "2005"),
            ("B", 1, 400, "1993"),
            ("B", 0, 500, "2022"),
            ("B", 3, None, "2016"),
        ]
        columns = ["types", "ordering", "value", "startYear"]
        return self.get_spark_session().createDataFrame(data, columns)

    def test_calculate_null_values_percentage_method(self):
        schema = t.StructType(
            [
                t.StructField("types", t.DoubleType(), True),
                t.StructField("ordering", t.DoubleType(), True),
                t.StructField("value", t.DoubleType(), True),
                t.StructField("startYear", t.StringType(), True),
            ]
        )
        expected_result = self.get_spark_session().createDataFrame(
            [tuple([0.0, 0.0, 33.33, 0.0])], schema=schema
        )
        df = self.create_test_data_frame()
        actual_result = base.calculate_null_values_percentage(df)
        self.assertEqual(
            actual_result.first()["value"],
            expected_result.first()["value"],
            "The percentage for value is wrong.",
        )

    def test_convert_string_to_date_method(self):
        df = self.create_test_data_frame()
        df = base.convert_string_to_date(df, Columns.START_YEAR, "yyyy")
        actual_result = [
            field.dataType
            for field in df.schema.fields
            if field.name == Columns.START_YEAR.value
        ][0]
        expected_result = t.DateType()
        self.assertEqual(
            actual_result,
            expected_result,
            f"StringType column '{Columns.START_YEAR.value}' hasn't been converted to DateType",
        )

    def test_zero_to_none_method(self):
        df = self.create_test_data_frame()
        df = base.zero_to_none(df, Columns.ORDERING)
        self.assertTrue(
            df.filter(col(Columns.ORDERING.value) == 0).count() == 0,
            f"Column '{Columns.ORDERING.value}' contains zero values",
        )

    def test_get_unique_values_for_categorical_columns_method(self):
        df = self.create_test_data_frame()
        df = base.get_unique_values_for_categorical_columns(df, Columns.TYPES)
        expected_result = [("B", 3), ("A", 3)]
        actual_result = df.select("types", "count(1)").orderBy("types").collect()
        self.assertEqual(sorted(actual_result), sorted(expected_result), "DataFrame content does not match the expected data.")

if __name__ == "__main__":
    unittest.main()
