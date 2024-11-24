def load_csv(spark, file_path: str):
    """
    Load a CSV file into a Spark DataFrame.
    
    Parameters:
    - spark: Active SparkSession.
    - file_path: The path to the CSV file.
    
    Returns:
    - Spark DataFrame containing the CSV data.
    """
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        #df.show(5)  # Optionally display a preview of the data
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        raise
