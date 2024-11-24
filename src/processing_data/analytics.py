def journal_with_most_drugs(spark):
    # Utiliser SQL pour trouver le journal qui mentionne le plus de médicaments différents
    result = spark.sql("""
        SELECT journal as journal_with_most_drugs 
        FROM drugs_journals
        GROUP BY journal
        ORDER BY COUNT(DISTINCT drug) DESC
        LIMIT 1
    """)
    return result


def drugs_in_same_journals(spark, drug_name):
    # Utiliser SQL pour trouver les médicaments mentionnés dans les mêmes journaux que le médicament donné,
    # mais uniquement dans les publications PubMed et exclure les tests cliniques (Clinical Trials)
    result = spark.sql(f"""
        SELECT DISTINCT d1.drug as drugs_in_same_journals
        FROM drugs_journals d1
        JOIN drugs_journals d2 ON d1.journal = d2.journal
        WHERE d1.drug != '{drug_name}'
        AND d1.type = 'pubmed'  -- PubMed publications
        AND d2.type = 'pubmed'  -- PubMed publications
        AND d2.drug = '{drug_name}'
    """)
    return result

