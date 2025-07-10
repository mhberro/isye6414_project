import pandas as pd
from functools import reduce
import urllib
from sqlalchemy import create_engine

conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=.\data\raw_data\IPEDS202223.accdb;"
)

quoted = urllib.parse.quote_plus(conn_str)
engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")

table_cols = {
    "IC2022_AY":   ["TUITION1", "TUITION2", "TUITION3", "CHG4AY0", "CHG4AY1", "CHG4AY2", "CHG4AY3"],
    "HD2022":      ["CONTROL", "ICLEVEL", "SECTOR", "LOCALE"],
    "SFA2122_P1":  ["LOAN_A", "FLOAN_A", "OLOAN_A", "AGRNT_A", "PGRNT_A"],
    "SFA2122_P2":  ["NPIST0", "NPIST1", "NPIST2", "NPIS412", "NPIS422", "NPIS432", "NPIS442", "NPIS452"],
    "IC2022_PY":   ["CIPCODE2", "CIPCODE3", "CIPTUIT2", "CIPTUIT3"],
}

# Pull each table into a DataFrame
dfs = []
for tbl, cols in table_cols.items():
    all_cols = ["[UNITID]"] + [f"[{c}]" for c in cols]
    sql = f"SELECT {', '.join(all_cols)} FROM [{tbl}]"
    df = pd.read_sql_query(sql, engine)
    dfs.append(df)

#Merge everything on UNITID. Used outer so we don’t lose any schools
merged = reduce(lambda a, b: pd.merge(a, b, on="UNITID", how="outer"), dfs)

out_path = r".\data\parsed_data\selected_socioeconomic_factors.csv"
merged.to_csv(out_path, index=False)
print(f"Wrote combined data to {out_path}")
