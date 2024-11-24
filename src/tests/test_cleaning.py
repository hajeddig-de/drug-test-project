import unittest
from src.cleaning_data.clean_drugs import clean_column_names, clean_data
import pandas as pd

def test_clean_column_names(self):
        df = pd.DataFrame(columns=["Column 1", " Column 2 "])
        result = clean_column_names(df)
        self.assertListEqual(result.columns.tolist(), ["column_1", "column_2"])

def test_clean_data(self):
        df = pd.DataFrame({"col": [1, 2, None, 2]})
        result = clean_data(df)
        self.assertEqual(len(result), 2)


