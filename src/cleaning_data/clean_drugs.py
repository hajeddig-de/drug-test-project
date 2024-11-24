from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def validate_schema(df, schema):
    """
    Validate and conform the input DataFrame to the given schema.

    Parameters:
    - df: Spark DataFrame.
    - schema: StructType defining the expected schema.

    Returns:
    - Spark DataFrame with types casted to match the schema.
    """
    for field in schema.fields:
        col_name = field.name
        col_type = field.dataType
        # If the column exists in the DataFrame, cast it to the correct type
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        else:
            # Add a missing column with null values of the required type
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
    return df

def clean_column_names(df):
    """
    Clean the column names by stripping leading/trailing spaces, converting to lowercase,
    and replacing spaces with underscores. Additionally, standardize the "date" column 
    to the format "MM/dd/yyyy".s

    Parameters:
    - df: Spark DataFrame.

    Returns:
    - Spark DataFrame with cleaned column names and formatted date column.
    """
    # Clean column names
    cleaned_columns = [F.col(col).alias(col.strip().lower().replace(' ', '_')) for col in df.columns]
    df = df.select(*cleaned_columns)

    # Format "date" column if it exists
    if "date" in df.columns:
        df = df.withColumn(
            "date",
            F.date_format(
                F.coalesce(
                    F.to_date("date", "d MMMM yyyy"),  # e.g., "1 January 2020"
                    F.to_date("date", "yyyy-MM-dd"),  # e.g., "2020-01-01"
                    F.to_date("date", "MM/dd/yyyy"),  # e.g., "01/01/2020"
                    F.to_date("date", "MM-dd-yyyy"),   # e.g., "01-01-2020"
                    F.to_date("date", "dd/MM/yyyy")   # e.g., "01-01-2020"
                ),
                "MM/dd/yyyy"
            )
        )

    return df


def clean_date_column(df, date_column="date"):
    """
    Clean and standardize the date column to 'MM/dd/yyyy' format.

    Parameters:
    - df: Spark DataFrame with a 'date' column.
    - date_column: Name of the column containing dates (default: 'date').

    Returns:
    - Spark DataFrame with standardized 'date' column.
    """
    # Define date formats to try
    date_formats = ["MM/dd/yyyy", "yyyy-MM-dd", "MM-dd-yyyy", "d MMMM yyyy"]

    # Try parsing the column with multiple formats and coalesce results
    df = df.withColumn(
        date_column,
        F.coalesce(
            *[F.date_format(F.to_date(F.col(date_column), fmt), "MM/dd/yyyy") for fmt in date_formats]
        )
    )

    # Replace any null values with a default date
    df = df.fillna({date_column: "01/01/1900"})

    return df

def clean_data(df, schema):
    """
    Perform basic cleaning of the data (e.g., removing missing values, type casting).

    Parameters:
    - df: Spark DataFrame.
    - schema: StructType defining the expected schema.

    Returns:
    - Spark DataFrame after cleaning.
    """
    # Clean column names
    df = clean_column_names(df)

    # Validate and cast the schema
    df = validate_schema(df, schema)

    # Remove rows with any null or missing values in mandatory fields
    mandatory_fields = [field.name for field in schema.fields if not field.nullable]
    df_cleaned = df.dropna(subset=mandatory_fields)

    return df_cleaned