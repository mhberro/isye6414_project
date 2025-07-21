# #!/usr/bin/env python3

import os
import sys
import argparse
import urllib.parse
import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from functools import reduce

class CollegeScorecardFactorExtractor:
    def __init__(self, db_path, keywords=None):
        self.db_path = os.path.abspath(os.path.normpath(db_path))
        if not os.path.isfile(self.db_path):
            raise FileNotFoundError(f"Table not found: {self.db_path}")
        
        # self.keywords = keywords or [
        #     'family income', 'tuition', 'cost', 'price', 'major', 'school type',
        #     'loan', 'debt', 'grant', 'financial', 'gender', 'race',
        #     'family', 'aid', 'residency', 'charge', 'expense', 'fee'
        # ]
        self.keywords = keywords or [
            'debt', 'state', 'family'
        ]

    def load_data_dictionary(self, dd_path):
        df = pd.read_excel(dd_path, sheet_name='Institution_Data_Dictionary')
        return df[['varName','varTitle']]

    def filter_var_records(self, df_vars):
        # varTitle filters
        df = df_vars.copy()
        df = df[~df['varTitle'].str.contains('in-district', case=False, na=False)]
        year_mask = df['varTitle'].str.contains(r'20\d{2}', case=False, na=False)

        # count distinct root-keyword matches via findall
        combined_pattern = re.compile(r"\b(?:" + '|'.join(re.escape(kw)+r"\w*" for kw in self.keywords) + r")\b", flags=re.IGNORECASE)
        df['matches'] = df['varTitle'].str.findall(combined_pattern)
  
        df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)) if lst is not None else 0)

        df = df[df['match_count'] > 0]
        return df[['varName']].drop_duplicates()

    def extract(self, db_year):
        """
        Perform extraction, then limit to 10 variables per keyword.
        Returns a dict mapping TableName -> list of varNames.
        """
        # Load and filter
        dd_path = '..\\data\\data_dictionary_college_scorecard\\CollegeScorecardDataDictionary.xlsx'
        df_vars = self.load_data_dictionary(dd_path)
        matched = self.filter_var_records(df_vars)

        # Attach varTitle so we can re-filter per keyword

        matched = matched.merge(
            df_vars[['varName', 'varTitle']].drop_duplicates('varName'),
            on='varName', how='inner'
        )

        # Pick up to 10 vars per keyword
        allowed_vars = set()
        for kw in self.keywords:
            # Regex: match root + any suffix
            pat = re.compile(r"\b" + re.escape(kw) + r"\w*\b", flags=re.IGNORECASE)
            subset = matched[matched['varTitle'].str.contains(pat, na=False)]
            unique_list = []
            for v in subset['varName']:
                if v not in unique_list:
                    unique_list.append(v)
                # if len(unique_list) == 5:
                #     break
            allowed_vars.update(unique_list)

        # Filter matched down to only those allowed
        matched = matched[matched['varName'].isin(allowed_vars)]

        # print(matched[:15])
        
        factors = sorted(matched['varName'].tolist())
        
        return factors, matched

def process_database(db_path, extractor):

    db_year = re.search(r'\d{4}', db_path).group(0)
    factors, matched = extractor.extract(db_year)
    df_tbl = pd.read_csv(db_path)
    print('column length before extraction:', df_tbl.shape[1])


    keep = ['UNITID'] + [c for c in factors if c in df_tbl.columns]
    df_tbl = df_tbl[keep]
    print('column length after extraction:', df_tbl.shape[1])
    
    initial_cols = df_tbl.shape[1]
    # final_merged_df = df_tbl
    final_merged_df = df_tbl.dropna(axis=1, thresh=df_tbl.shape[0] // 2)

    dd_processed_path = f'..\\data\\processed_data\\data_dictionary\\college_scorecard\\{db_year}.csv'
    os.makedirs(os.path.dirname(dd_processed_path), exist_ok=True)
    matched[matched['varName'].isin(final_merged_df.columns)].to_csv(dd_processed_path, index=False)

    print(f"Dropped {initial_cols - final_merged_df.shape[1]} sparse columns.")

    return final_merged_df

def main():
    input_dir = '..\\data\\raw_data\\college_scorecard\\'
    output_file = '..\\data\\processed_data\\extracted_college_scorecard_socioeconomic_features.csv'

    input_dir = os.path.normpath(input_dir)
    if not os.path.isdir(input_dir): sys.exit(f"Dir not found: {input_dir}")
    first = next((f for f in os.listdir(input_dir) if f.lower().endswith('.csv')), None)
    if not first: sys.exit("No DBs.")
    extractor = CollegeScorecardFactorExtractor(os.path.join(input_dir, first))
    
    dfs = []
    for f in os.listdir(input_dir):
        if not f.lower().endswith('.csv'): continue

        df = process_database(os.path.join(input_dir, f), extractor)
        print('c')
        if df is not None:
            year = int(re.search(r'MERGED(\d{4})', f).group(1))
            df['year'] = year
            dfs.append(df)

    if not dfs: sys.exit("No data.")
    combined = pd.concat(dfs, ignore_index=True, sort=False)
    initial_cols = combined.shape[1]
    combined.dropna(axis=1, thresh=combined.shape[0] // 2)
    print(f"Final Dataframe: Dropped {initial_cols - combined.shape[1]} sparse columns.")
    combined['year'] = combined.pop('year')

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    combined.to_csv(output_file, index=False)
    print(f"Merged CSV written to {output_file}, shape {combined.shape}.")

if __name__=='__main__':
    main()