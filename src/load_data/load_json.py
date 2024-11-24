from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

def load_json(spark: SparkSession, filepath: str, schema: StructType) -> DataFrame:
    """
    Load JSON data into a Spark DataFrame with a specified schema.

    Parameters:
    - spark: Active Spark session.
    - filepath: Path to the JSON file.
    - schema: StructType defining the schema of the JSON data.

    Returns:
    - Spark DataFrame containing the loaded JSON data.
    """
    # Read the JSON file with the specified schema and return the resulting DataFrame
    df = spark.read.option("multiline", "true").schema(schema).json(filepath)
    
    # Return the DataFrame
    #df.show()
    return df
