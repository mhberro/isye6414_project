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

class IPEDSSocioeconomicFactorExtractor:

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
        relevant_tables.append(f'HD{db_year}')
        if relevant_tables:
            df = df[df['TableName'].isin(relevant_tables)]
        # count distinct root-keyword matches via findall
        combined_pattern = re.compile(r"\b(?:" + '|'.join(re.escape(kw)+r"\w*" for kw in self.keywords) + r")\b", flags=re.IGNORECASE)
        df['matches'] = df['longDescription'].str.findall(combined_pattern)

        # df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)))        
        df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)) if lst is not None else 0)
        
        df_matched = df[df['match_count'] >= 2]

        hd_tables = [t for t in relevant_tables if t.upper().startswith('HD')]
        if hd_tables:
            extra = df_vars[
                df_vars['TableName'].isin(hd_tables) & (df_vars['varName'] == 'STABBR')
            ].copy()

            extra['matches'] = extra['longDescription'].str.findall(combined_pattern)
            extra['match_count'] = extra['matches'].apply(lambda lst: len({m.lower() for m in lst}))

            df_matched = pd.concat([df_matched, extra], ignore_index=True)

        return df_matched[['TableName','varName']].drop_duplicates()

    def extract(self, db_year, exclusions=None):
        # Load and filter
        df_titles = self.load_table_titles(db_year[-2:])
        relevant_tables = self.filter_relevant_tables(df_titles)
        df_vars = self.load_vartable(db_year[-2:])
        matched = self.filter_var_records(df_vars, relevant_tables, db_year)
        # mapping = None
        
        # Attach longDescription so we can re-filter per keyword
        matched = matched.merge(
            df_vars[['varName', 'longDescription']].drop_duplicates('varName'),
            on='varName', how='left'
        )

        exclusions = exclusions or ['STABBR']
        allowed_vars = set(exclusions)

        # Pick up to 10 vars per keyword
        # allowed_vars = set()
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
            'debt', 'family income'
        ]

    def load_data_dictionary(self, dd_path):
        df = pd.read_excel(dd_path, sheet_name='Institution_Data_Dictionary')
        return df[['varName','varTitle']]

    def filter_var_records(self, df_vars, exclusions=None):
        # varTitle filters
        df = df_vars.copy()
        df = df[~df['varTitle'].str.contains('in-district', case=False, na=False)]
        year_mask = df['varTitle'].str.contains(r'20\d{2}', case=False, na=False)

        exclusions = exclusions if exclusions is not None else ['STABBR']

        # count distinct root-keyword matches via findall
        combined_pattern = re.compile(r"\b(?:" + '|'.join(re.escape(kw)+r"\w*" for kw in self.keywords) + r")\b", flags=re.IGNORECASE)
        df['matches'] = df['varTitle'].str.findall(combined_pattern)
  
        df['match_count'] = df['matches'].apply(lambda lst: len(set(m.lower() for m in lst)) if lst is not None else 0)

        df_matched_by_keywords  = df[df['match_count'] > 0]
        df_explicit_inclusions = df_vars[df_vars['varName'].isin(exclusions)].copy()

        final_df = pd.concat([
            df_matched_by_keywords[['varName']],
            df_explicit_inclusions[['varName']]
        ]).drop_duplicates()
        return final_df.drop_duplicates()

    def extract(self, db_year, exclusions=None):
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

        exclusions = exclusions or ['STABBR']
        allowed_vars = set(exclusions)

        # Pick up to 10 vars per keyword
        # allowed_vars = set()
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

class IPEDSSocioeconomicFactors:
    def __init__(self):
        pass

    def get_table_columns(self, engine, table_name):
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT * FROM [{table_name}] WHERE 1=0"))
            return list(res.keys())


    def read_table_in_chunks(self, engine, table_name, desired_cols, chunk_size=5000):
        avail = self.get_table_columns(engine, table_name)
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


    def process_database(self, db_path, extractor):
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
            chunks = list(self.read_table_in_chunks(extractor.engine, tbl, cols))
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

    def main(self):
        input_dir = '..\\data\\raw_data\\ipeds\\'
        output_file = '..\\data\\processed_data\\extracted_ipeds_socioeconomic_features.csv'

        input_dir = os.path.normpath(input_dir)
        if not os.path.isdir(input_dir): sys.exit(f"Dir not found: {input_dir}")
        first = next((f for f in os.listdir(input_dir) if f.lower().endswith('.accdb')), None)
        if not first: sys.exit("No DBs.")
        extractor = IPEDSSocioeconomicFactorExtractor(os.path.join(input_dir, first))
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

            df = self.process_database(os.path.join(input_dir, f), extractor)
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

        return combined

class CollegeScorecardSocioeconomicFactors:
    def __init__(self):
        pass

    def process_database(self, db_path, extractor):

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

    def main(self):
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

            df = self.process_database(os.path.join(input_dir, f), extractor)
            
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

        return combined

def load_shef_data():
    input_dir = '..\\data\\raw_data\\shef\\'

    input_dir = os.path.normpath(input_dir)
    if not os.path.isdir(input_dir): sys.exit(f"Dir not found: {input_dir}")

    first = next((f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')), None)
    if not first: sys.exit("No DBs.")
    shef_path = os.path.join(input_dir, first)
    df = pd.read_excel(shef_path, sheet_name='Report Data Reduced')

    shef_df = df[['STABBR', 'year', 'HECA_Inflation_Adjustment', 'CPI_Inflation_Adjustment', 
                'Total_State_Support', 'Total_Financial_Aid', 'Education_Appropriations', 
                'State_Public_Financial_Aid_as_a_Percent_of_Education_Appropriations',
                'Net_Tuition_and_Fee_Revenue', 'Student_Share', 'Net_FTE_Enrollment', 'State_Public_Financial_Aid']]
    
    shef_df = shef_df[shef_df['STABBR'] != 'U.S.']
    return shef_df

if __name__=='__main__':
    output_file = '..\\data\\processed_data\\extracted_ipeds_cs_shef_socioeconomic_features.csv'
    ipeds_factors = IPEDSSocioeconomicFactors()
    ipeds_df = ipeds_factors.main()

    college_scorecard_factors = CollegeScorecardSocioeconomicFactors()
    college_scorecard_df = college_scorecard_factors.main()

    shef_df = load_shef_data()
    # shef_2020_df = shef_df[shef_df['year'] == 2020]

    print('IPEDS shape:', ipeds_df.shape)
    print('College Scorecard shape:', college_scorecard_df.shape)
    print('SHEF shape:', shef_df.shape)

    ipeds_cs_factors = ipeds_df.merge(college_scorecard_df, on=['UNITID', 'STABBR', 'year'], how='inner')
    print('IPEDS_College Scorecard Combined shape:', ipeds_cs_factors.shape)
    
    ipeds_cs_shef_factors = ipeds_cs_factors.merge(shef_df, on=['STABBR', 'year'], how='inner')
    print('IPEDS_College Scorecard_SHEF Combined shape:', ipeds_cs_shef_factors.shape)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    ipeds_cs_shef_factors.to_csv(output_file, index=False)
    print(f"Merged CSV written to {output_file}, shape {ipeds_cs_shef_factors.shape}.")


