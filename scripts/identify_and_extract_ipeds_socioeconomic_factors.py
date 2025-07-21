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

class SocioeconomicFactorExtractor:

    def __init__(self, db_path, keywords=None):
        self.db_path = os.path.abspath(os.path.normpath(db_path))
        if not os.path.isfile(self.db_path):
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        conn_str = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={self.db_path};"
        )
        quoted = urllib.parse.quote_plus(conn_str)

        self.engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")
        self.keywords = keywords or [
            'income', 'tuition', 'cost', 'price', 'major', 'school type',
            'loan', 'debt', 'grant', 'financial', 'gender', 'race',
            'family', 'aid', 'residency', 'charge', 'expense', 'fee'
        ]

    def load_table_titles(self, db_year):
        df = pd.read_sql_table(f'Tables{db_year}', self.engine)
        req = {'TableName','TableTitle'}
        if not req.issubset(df.columns):
            raise KeyError(f"Tables{db_year} missing: {req - set(df.columns)}")
        return df[['TableName','TableTitle']]

    def filter_relevant_tables(self, df_titles):
        pattern = '|'.join(fr"\b{re.escape(kw)}\w*\b" for kw in self.keywords)
        mask = df_titles['TableTitle'].str.contains(pattern, case=False, na=False, regex=True)
        return df_titles.loc[mask,'TableName'].unique().tolist()

    def load_vartable(self, db_year):
        df = pd.read_sql_table(f'vartable{db_year}', self.engine)
        req = {'TableName','varName','varTitle','longDescription'}
        if not req.issubset(df.columns):
            raise KeyError(f"vartable{db_year} missing: {req - set(df.columns)}")
        return df

    def filter_var_records(self, df_vars, relevant_tables, db_year):
        # varTitle filters
        df = df_vars.copy()
        df = df[~df['varTitle'].str.contains('in-district', case=False, na=False)]
        year_mask = df['varTitle'].str.contains(r'20\d{2}', case=False, na=False)
        df = df[~(year_mask & ~df['varTitle'].str.contains(db_year, case=False, na=False))]
        if relevant_tables:
            df = df[df['TableName'].isin(relevant_tables)]
        # count distinct root-keyword matches via findall
        combined_pattern = re.compile(r"\b(?:" + '|'.join(re.escape(kw)+r"\w*" for kw in self.keywords) + r")\b", flags=re.IGNORECASE)
        df['matches'] = df['longDescription'].str.findall(combined_pattern)

        # df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)))        
        df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)) if lst is not None else 0)
        
        df = df[df['match_count'] >= 2]
        return df[['TableName','varName']].drop_duplicates()

    def extract(self, db_year):
        # Load and filter
        df_titles = self.load_table_titles(db_year[-2:])
        relevant_tables = self.filter_relevant_tables(df_titles)
        df_vars = self.load_vartable(db_year[-2:])
        matched = self.filter_var_records(df_vars, relevant_tables, db_year)

        # Attach longDescription so we can re-filter per keyword
        matched = matched.merge(
            df_vars[['varName', 'longDescription']].drop_duplicates('varName'),
            on='varName', how='left'
        )

        # Pick up to 10 vars per keyword
        allowed_vars = set()
        for kw in self.keywords:
            # Regex: match root + any suffix
            pat = re.compile(r"\b" + re.escape(kw) + r"\w*\b", flags=re.IGNORECASE)
            subset = matched[matched['longDescription'].str.contains(pat, na=False)]
            unique_list = []
            for v in subset['varName']:
                if v not in unique_list:
                    unique_list.append(v)
                # if len(unique_list) == 5:
                #     break
            allowed_vars.update(unique_list)

        # Filter matched down to only those allowed
        matched = matched[matched['varName'].isin(allowed_vars)]

        # Build final mapping
        mapping = {}
        for tbl, group in matched.groupby('TableName'):
            mapping[tbl] = sorted(group['varName'].tolist())
        return mapping, matched



def get_table_columns(engine, table_name):
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT * FROM [{table_name}] WHERE 1=0"))
        return list(res.keys())


def read_table_in_chunks(engine, table_name, desired_cols, chunk_size=5000):
    avail = get_table_columns(engine, table_name)
    if 'UNITID' not in avail:
        return
    found = [c for c in desired_cols if c in avail]
    missing = [c for c in desired_cols if c not in avail]
    cols = ['UNITID'] + found
    sql = f"SELECT {','.join(f'[{c}]' for c in cols)} FROM [{table_name}]"
    for chunk in pd.read_sql_query(sql, engine, chunksize=chunk_size):
        for dt, ndt in [('float64','float32'),('int64','int32')]:
            for c in chunk.select_dtypes(include=[dt]): chunk[c] = chunk[c].astype(ndt)
        for c in missing: chunk[c] = pd.NA
        yield chunk


def process_database(db_path, extractor):
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
    )
    engine = create_engine(
        f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
    )
    db_year = re.search(r'\d{4}', db_path).group(0)
    tbl_map, matched = extractor.extract(db_year)

    # Load data from database
    table_dfs = []
    for tbl, cols in tbl_map.items():
        chunks = list(read_table_in_chunks(extractor.engine, tbl, cols))
        if not chunks:
            continue
        df_tbl = pd.concat(chunks, ignore_index=True)
        keep = ['UNITID'] + [c for c in cols if c in df_tbl.columns]
        table_dfs.append(df_tbl[keep])

    # Pre-process DataFrames by Aggregating
    print("\n CLEANING DATA (AGGREGATING TO CREATE UNIQUE UNITIDs)")
    cleaned_dfs = []
    seen_columns = {'UNITID'}
    
    for df in table_dfs:
        # Check if aggregation is needed
        if df['UNITID'].is_unique:
            df_clean = df
            # print(f"Table for columns {df.columns.tolist()} has unique UNITIDs. No aggregation needed.")
        else:
            # print(f"Table for columns {df.columns.tolist()} has duplicate UNITIDs. Aggregating now...")
            # AGGREGATION LOGIC 
            # Build a dictionary to tell pandas how to aggregate each column.
            agg_dict = {}
            for col in df.columns:
                if col == 'UNITID':
                    continue
                # For numeric columns take the mean. 
                if pd.api.types.is_numeric_dtype(df[col]):
                    agg_dict[col] = 'mean'
                # For non-numeric columns just take the first value encountered.
                else:
                    agg_dict[col] = 'first'
            
            # print(f"\t- Aggregation strategy: {agg_dict}")
            df_clean = df.groupby('UNITID', as_index=False).agg(agg_dict)
            # print(f"\t- Shape before aggregation: {df.shape} -> After: {df_clean.shape}")

        # Drop overlapping columns before adding to the list for merging
        overlap_cols = [col for col in df_clean.columns if col in seen_columns and col != 'UNITID']
        if overlap_cols:
            df_clean = df_clean.drop(columns=overlap_cols)
        
        seen_columns.update(df_clean.columns)
        cleaned_dfs.append(df_clean)

    # 3. Merge the aggregated dataframes
    print(f"\nMERGING {len(cleaned_dfs)} CLEANED DATAFRAMES")
    final_merged_df = reduce(lambda left, right: pd.merge(left, right, on='UNITID', how='outer'), cleaned_dfs)
    print("Merge successful. Final shape:", final_merged_df.shape)

    # Final Optimizations

    initial_cols = final_merged_df.shape[1]
    final_merged_df = final_merged_df.dropna(axis=1, thresh=final_merged_df.shape[0] // 2)
    print(f"Dropped {initial_cols - final_merged_df.shape[1]} sparse columns.")
    
    for dt, ndt in [('float64', 'float32'), ('int64', 'int32')]:
        for col in final_merged_df.select_dtypes(include=[dt]):
            final_merged_df[col] = final_merged_df[col].astype(ndt)

    dd_processed_path = f'..\\data\\processed_data\\data_dictionary\\ipeds\\{db_year}.csv'
    os.makedirs(os.path.dirname(dd_processed_path), exist_ok=True)
    matched[matched['varName'].isin(final_merged_df.columns)].to_csv(dd_processed_path, index=False)

    return final_merged_df

def main():
    input_dir = '..\\data\\raw_data\\ipeds\\'
    output_file = '..\\data\\processed_data\\extracted_ipeds_socioeconomic_features.csv'

    input_dir = os.path.normpath(input_dir)
    if not os.path.isdir(input_dir): sys.exit(f"Dir not found: {input_dir}")
    first = next((f for f in os.listdir(input_dir) if f.lower().endswith('.accdb')), None)
    if not first: sys.exit("No DBs.")
    extractor = SocioeconomicFactorExtractor(os.path.join(input_dir, first))
    # print('!!! FIRST !!!', os.path.join(input_dir, first))

    # if details_csv:
    #     df_det = extractor.extract_details(details_csv)
    #     print(f"Detail CSV written to {details_csv}, {len(df_det)} rows.")
    dfs = []
    for f in os.listdir(input_dir):
        if not f.lower().endswith('.accdb'): continue
        if first != f: 
            db_path = os.path.join(input_dir, f)
            db_path = os.path.abspath(os.path.normpath(db_path))

            conn_str = (
                r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
                f"DBQ={db_path};"
            )
            quoted = urllib.parse.quote_plus(conn_str)
            extractor.engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")

        df = process_database(os.path.join(input_dir, f), extractor)
        if df is not None:
            year = int(re.search(r'IPEDS(\d{4})', f).group(1))
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

