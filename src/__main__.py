from pyspark.sql import SparkSession
from src.load_data.load_csv import load_csv
from src.load_data.load_json import load_json
from src.cleaning_data.clean_drugs import clean_data, clean_column_names
from src.cleaning_data.clean_pubmed import clean_pubmed_data
from src.processing_data.create_graph import create_graph
from src.processing_data.analytics import journal_with_most_drugs, drugs_in_same_journals
from src.utils.logger import setup_logger
from src.utils.config import load_config
from pyspark.sql.functions import explode, col
from src.schema import schemas
from src.tests.test_cleaning import  test_clean_column_names, test_clean_data
import os
import pandas as pd
import json
import unittest
import sys

logger = setup_logger()

def run_all_tests():
    """Discover and run all tests."""
    logger.info("Running all tests...")
    # Discover tests in the `src/tests` directory
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('src/tests', pattern='test_*.py')

    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    if result.wasSuccessful():
        logger.info("All tests passed successfully!")
    else:
        logger.error("Some tests failed. Check the logs for details.")
        raise SystemExit("Exiting due to test failures.")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DataPipeline") \
        .getOrCreate()
    
    try:
        # Check if 'test' argument is provided to run tests instead of pipeline
        if len(sys.argv) > 1 and sys.argv[1] == "test":
            run_all_tests()
            return
        # Load config
        config = load_config()
        
        # Access paths from the config
        source_path = config["source"]["path"]
        drug_file = config["source"]["drug_file"]
        clinical_trial_file = config["source"]["clinical_trial_file"]
        pubmed_csv_file = config["source"]["pubmed_csv_file"]
        pubmed_json_file = config["source"]["pubmed_json_file"]
        
        output_path = config["output"]["path"]
        drug_graph_file = config["output"]["drug_graph"]
        top_journals_file = config["output"]["top_journals"]
        
        logger.info("Démarrage du pipeline de données...")
        
        # Charger et nettoyer les données des médicaments
        logger.info("1 === Chargement et nettoyage des données des médicaments...")
        drugs = clean_data(clean_column_names(load_csv(spark, os.path.join(source_path, drug_file))),schemas.drug_schema)
        
        #drugs.show()
        # Uncomment other steps as needed and make similar adjustments
        
        # Example for PubMed CSV and JSON loading
        
        logger.info("2 === Chargement et nettoyage des données PubMed...")
        pubmed_csv = clean_pubmed_data(load_csv(spark, os.path.join(source_path, pubmed_csv_file)))
        pubmed_json = clean_pubmed_data(load_json(spark, os.path.join(source_path, pubmed_json_file),schemas.pubmed_schema))
        
        logger.info("3 === Fusion des données PubMed (CSV + JSON)...")
        pubmed_combined = pubmed_csv.union(pubmed_json)
        
        logger.info("4 === Chargement et nettoyage des données des essais cliniques...")
        clinical_trials = clean_data(clean_column_names(load_csv(spark, os.path.join(source_path, clinical_trial_file))),schemas.clinical_trial_schema)

        # Créer le graphe
        logger.info("5 === Construction du graphe...")
        graph_json_final = create_graph(drugs, pubmed_combined, clinical_trials)

        # Convertir les chaînes JSON échappées en objets Python
        graph_json = [json.loads(item) for item in graph_json_final]
        
        # Définir le chemin de sortie
        output_path = os.path.join(output_path, drug_graph_file)

        # Écrire les données dans un fichier JSON proprement formaté
        with open(output_path, "w") as f:
            json.dump(graph_json, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Done ===> Graphe JSON sauvegardé dans {output_path}")
        logger.info("Pipeline terminé avec succès.")

        # Ad-hoc Extraire le nom du journal qui mentionne le plus de médicaments différents.
    
        # Charger les données JSON dans un DataFrame Spark
        df = spark.read.json(spark.sparkContext.parallelize(graph_json_final))

        # Aplatir la structure des mentions
        df_flat = df.withColumn("mention", explode(col("mentions"))) \
            .select("drug", col("mention.journal"), col("mention.date"), col("mention.type"), col("mention.title"))

        # Afficher le DataFrame plat
        df_flat.show()
        
        # Créer une vue temporaire pour SQL
        df_flat.createOrReplaceTempView("drugs_journals")

        
        journal = journal_with_most_drugs(spark)
        journal.show()

        
        drugs = drugs_in_same_journals(spark, "betamethasone")
        drugs.show()
        
    except Exception as e:
        logger.error(f"Erreur dans le pipeline: {e}")
        raise
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
