from pyspark.sql import functions as F
import json

def create_graph(drugs_df, pubmed_df, clinical_trials_df):
    """
    Construct a graph by linking drugs, pubmed articles, and clinical trials,
    and generate a JSON representation of the graph with drugs, journals, and mentions.
    """
    
    # Normalisation des noms de médicaments en minuscules
    drugs_df = drugs_df.withColumn("drug_name", F.lower(F.col("drug")).alias("drug_name"))

    # Nettoyage des données PubMed
    pubmed_df = pubmed_df.withColumn("title_lower", F.lower(F.col("title"))) \
                         .withColumnRenamed("id", "pubmed_id") \
                         .withColumnRenamed("journal", "pubmed_journal") \
                         .withColumnRenamed("date", "pubmed_date")

    # Nettoyage des données Clinical Trials
    clinical_trials_df = clinical_trials_df.withColumnRenamed("journal", "clinical_journal") \
                                           .withColumnRenamed("date", "clinical_date") \
                                           .withColumnRenamed("scientific_title", "clinical_scientific_title")

    # Jointure avec PubMed pour obtenir les mentions
    pubmed_mentions_df = drugs_df.join(
        pubmed_df, 
        F.lower(pubmed_df["title"]).contains(F.lower(drugs_df["drug_name"])),
        how="inner"
    ).select(
        "drug_name", "pubmed_journal", "pubmed_date", "title", F.lit("pubmed").alias("type")
    )

    # Jointure avec Clinical Trials pour obtenir les mentions
    clinical_mentions_df = drugs_df.join(
        clinical_trials_df, 
        F.lower(clinical_trials_df["clinical_scientific_title"]).contains(F.lower(drugs_df["drug_name"])),
        how="inner"
    ).select(
        "drug_name", "clinical_journal", "clinical_date", "clinical_scientific_title", F.lit("clinical").alias("type")
    )

    # Aligner les colonnes pour les deux DataFrames avant l'union
    pubmed_mentions_df = pubmed_mentions_df.withColumn("journal", F.col("pubmed_journal")) \
                                           .withColumn("date", F.col("pubmed_date")) \
                                           .withColumn("title", F.col("title")) \
                                           .drop("pubmed_journal", "pubmed_date")
    
    clinical_mentions_df = clinical_mentions_df.withColumn("journal", F.col("clinical_journal")) \
                                               .withColumn("date", F.col("clinical_date")) \
                                               .withColumn("title", F.col("clinical_scientific_title")) \
                                               .drop("clinical_journal", "clinical_date", "clinical_scientific_title")

    # Union des résultats
    mentions_df = pubmed_mentions_df.unionByName(clinical_mentions_df)

    # Créer un DataFrame structuré avec les informations nécessaires
    mentions_df = mentions_df.withColumn(
        "title", 
        F.when(mentions_df["type"] == "pubmed", mentions_df["title"]).otherwise(mentions_df["title"])
    ).withColumn(
        "journal", 
        F.when(mentions_df["type"] == "pubmed", mentions_df["journal"]).otherwise(mentions_df["journal"])
    ).withColumn(
        "date", 
        F.when(mentions_df["type"] == "pubmed", mentions_df["date"]).otherwise(mentions_df["date"])
    )

    # Agrégation des mentions par médicament
    result_df = mentions_df.groupBy("drug_name").agg(
        F.collect_list(
            F.struct("journal", "date", "type", "title")
        ).alias("mentions")
    )

    # Convertir en JSON
    result_json = result_df.rdd.map(lambda row: json.dumps({
        "drug": row["drug_name"],
        "mentions": [{"journal": mention["journal"], "date": mention["date"], "type": mention["type"], "title": mention["title"]} for mention in row["mentions"]]
    })).collect()

    #print(result_json)
    return result_json
