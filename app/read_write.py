from pyspark.sql import DataFrame

def read(path_to_file, schema, spark_session):
    return spark_session.read.csv(
        path_to_file,
        sep=r"\t",
        header=True,
        nullValue="\\N",
        schema=schema
    )

def write(df: DataFrame, path_to_file: str):
    df.write.csv(path_to_file)
