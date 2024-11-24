from pyspark.sql.types import StructField, StructType, StringType



drug_schema = StructType(
    [
        StructField("atccode", StringType()),
        StructField("drug", StringType()),
    ]
)

clinical_trial_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("scientific_title", StringType()),
        StructField("date", StringType()),
        StructField("journal", StringType()),
    ]
)

pubmed_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("date", StringType()),
        StructField("journal", StringType()),
    ]
)