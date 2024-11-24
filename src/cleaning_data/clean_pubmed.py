from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def clean_pubmed_data(df: DataFrame) -> DataFrame:
    """
    Clean the PubMed data, handle date formats, and ensure correct column types.

    Parameters:
    - df: Spark DataFrame (PubMed data).

    Returns:
    - Spark DataFrame with cleaned data.
    """
    # Remove rows with null or missing values for critical columns
    df_cleaned = df.dropna(subset=["id", "title", "journal"])

    # Convert columns to appropriate data types
    df_cleaned = df_cleaned.withColumn("id", F.col("id").cast("string"))
    df_cleaned = df_cleaned.withColumn("title", F.col("title").cast("string"))
    df_cleaned = df_cleaned.withColumn("journal", F.col("journal").cast("string"))

    # Handle the "date" column by using multiple date formats with Spark's to_date function
    date_formats = ["MM/dd/yyyy", "yyyy-MM-dd", "MM-dd-yyyy"]

    # Try parsing the date column with each format
    for fmt in date_formats:
        df_cleaned = df_cleaned.withColumn(
            "date", 
            F.when(F.col("date").isNull(), F.to_date(F.col("date"), fmt)).otherwise(F.col("date"))
        )

    # After parsing, convert the date format from yyyy-MM-dd to MM/dd/yyyy if necessary
    df_cleaned = df_cleaned.withColumn(
        "date", 
        F.when(
            F.col("date").rlike(r'^\d{4}-\d{2}-\d{2}$'),  # Check if the date is in yyyy-MM-dd format
            F.date_format(F.col("date"), "MM/dd/yyyy")  # Convert to MM/dd/yyyy format
        ).otherwise(F.col("date"))
    )

    # Fallback: If still null, set a default date
    df_cleaned = df_cleaned.withColumn("date", F.coalesce(F.col("date"), F.lit("01/01/1900")))

    # Show cleaned data
    #df_cleaned.show()
    return df_cleaned
